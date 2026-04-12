/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"
#include "moxygen/MoQFilters.h"

namespace {
constexpr uint8_t kDefaultUpstreamPriority = 128;
}

namespace moxygen {

// Sends SUBSCRIBE_UPDATE to update forwarding state. Called from:
// - subscribeAnnounces: forwarder was empty, new subscriber added
// (forward=true)
// - subscribe: first forwarding subscriber added (forward=true)
// - onEmpty: last subscriber left a publish subscription (forward=false)
// - forwardChanged: forwarding subscriber count changed
folly::coro::Task<void> MoQRelay::doSubscribeUpdate(
    std::shared_ptr<Publisher::SubscriptionHandle> handle,
    bool forward) {
  auto updateRes = co_await handle->subscribeUpdate(
      {RequestID(0),
       handle->subscribeOk().requestID,
       kLocationMin,
       kLocationMax.group,
       kDefaultPriority,
       /*forward=*/forward,
       {}});
  if (updateRes.hasError()) {
    XLOG(ERR) << "subscribeUpdate failed: " << updateRes.error().reasonPhrase;
  }
}

std::shared_ptr<MoQRelay::AnnounceNode> MoQRelay::findNamespaceNode(
    const TrackNamespace& ns,
    bool createMissingNodes,
    MatchType matchType,
    std::vector<std::pair<std::shared_ptr<MoQSession>, bool>>* sessions) {
  std::shared_ptr<AnnounceNode> nodePtr(
      std::shared_ptr<void>(), &announceRoot_);
  for (auto i = 0ul; i < ns.size(); i++) {
    if (sessions) {
      // Extract session pointers with their forward preferences from the map
      for (const auto& [session, forward] : nodePtr->sessions) {
        sessions->emplace_back(session, forward);
      }
    }
    auto& name = ns[i];
    auto it = nodePtr->children.find(name);
    if (it == nodePtr->children.end()) {
      if (createMissingNodes) {
        auto node = std::make_shared<AnnounceNode>(*this, nodePtr.get());
        nodePtr->children.emplace(name, node);
        // Don't increment yet - only when content is actually added
        nodePtr = std::move(node);
      } else if (
          matchType == MatchType::Prefix && nodePtr.get() != &announceRoot_) {
        return nodePtr;
      } else {
        XLOG(ERR) << "prefix not found in announce tree";
        return nullptr;
      }
    } else {
      nodePtr = it->second;
    }
  }
  return nodePtr;
}

folly::coro::Task<Subscriber::AnnounceResult> MoQRelay::announce(
    Announce ann,
    std::shared_ptr<Subscriber::AnnounceCallback> callback) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace;
  // check auth
  if (!ann.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    co_return folly::makeUnexpected(
        AnnounceError{
            ann.requestID, AnnounceErrorCode::UNINTERESTED, "bad namespace"});
  }
  std::vector<std::pair<std::shared_ptr<MoQSession>, bool>> sessions;
  auto nodePtr = findNamespaceNode(
      ann.trackNamespace,
      /*createMissingNodes=*/true,
      MatchType::Exact,
      &sessions);

  // Log if there is already a session that has announced this track
  if (nodePtr->sourceSession) {
    XLOG(WARNING) << "Announce: Existing session ("
                  << nodePtr->sourceSession.get()
                  << ") has already announced trackNamespace="
                  << ann.trackNamespace;
    // Since we don't fully support multiple publishers -- cancel the old
    // announcement and remove ongoing subscriptions to this publisher
    // in that namespace.  Note: it could have announced a more specific
    // namespace which hasn't been overridden by the new publisher, but
    // for now we don't support that.
    if (nodePtr->announceCallback) {
      nodePtr->announceCallback->announceCancel(
          AnnounceErrorCode::CANCELLED, "New publisher");
      nodePtr->announceCallback.reset();
    }
    for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
      // Check if the subscription's FullTrackName is in this namespace
      if (it->first.trackNamespace.startsWith(ann.trackNamespace) &&
          it->second.upstream == nodePtr->sourceSession) {
        XLOG(DBG4) << "Erasing subscription to " << it->first;
        it = subscriptions_.erase(it);
      } else {
        ++it;
      }
    }

    nodePtr->sourceSession.reset();
  }

  // TODO: store auth for forwarding on future SubscribeAnnounces?
  auto session = MoQSession::getRequestSession();
  bool wasEmpty = !nodePtr->hasLocalSessions();
  nodePtr->sourceSession = std::move(session);
  nodePtr->announceCallback = std::move(callback);
  nodePtr->trackNamespace_ = ann.trackNamespace;
  nodePtr->setAnnounceOk({ann.requestID, {}});

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }
  for (auto& [outSession, forward] : sessions) {
    if (outSession != session) {
      auto exec = outSession->getExecutor();
      co_withExecutor(exec, announceToSession(outSession, ann, nodePtr))
          .start();
    }
  }
  co_return nodePtr;
}

folly::coro::Task<void> MoQRelay::announceToSession(
    std::shared_ptr<MoQSession> session,
    Announce ann,
    std::shared_ptr<AnnounceNode> nodePtr) {
  auto announceHandle = co_await session->announce(ann);
  if (announceHandle.hasError()) {
    XLOG(ERR) << "Announce failed err=" << announceHandle.error().reasonPhrase;
  } else {
    // This can race with unsubscribeAnnounces
    nodePtr->announcements[session] = std::move(announceHandle.value());
  }
}

// AnnounceNode ref count management methods for pruning
void MoQRelay::AnnounceNode::incrementActiveChildren() {
  activeChildCount_++;
  // Propagate up if this was the first active child
  if (activeChildCount_ == 1 && parent_ && !hasLocalSessions()) {
    parent_->incrementActiveChildren();
  }
}

void MoQRelay::AnnounceNode::decrementActiveChildren() {
  XCHECK_GT(activeChildCount_, 0);
  activeChildCount_--;
}

// Walk up the tree to find and prune the highest empty ancestor
void MoQRelay::AnnounceNode::tryPruneChild(const std::string& childKey) {
  auto it = children.find(childKey);
  if (it == children.end()) {
    return;
  }

  auto childNode = it->second.get();
  if (childNode->shouldKeep()) {
    return;
  }

  // Walk up the tree, decrementing counts, to find highest empty ancestor
  // Track the key to remove and the parent to remove it from
  std::string keyToRemove = childKey;
  AnnounceNode* parentOfNodeToRemove = this;
  AnnounceNode* current = this;

  while (current) {
    XCHECK_GT(current->activeChildCount_, 0);
    current->activeChildCount_--;

    // If current still has content or children after decrement, stop walking
    // Remove keyToRemove from parentOfNodeToRemove
    if (current->hasLocalSessions() || current->activeChildCount_ > 0) {
      break;
    }

    // Current is now empty too - if it has a parent, continue walking up
    if (!current->parent_) {
      // We've reached root - can't remove root, so stop
      break;
    }

    // Current is empty and not root, so it becomes the new candidate for
    // removal Find the key for current in its parent's children map
    for (const auto& [key, node] : current->parent_->children) {
      if (node.get() == current) {
        keyToRemove = key;
        parentOfNodeToRemove = current->parent_;
        break;
      }
    }

    current = current->parent_;
  }

  // Remove the highest empty node from its parent
  XLOG(DBG1) << "Pruning empty subtree at: " << keyToRemove;
  parentOfNodeToRemove->children.erase(keyToRemove);
}

void MoQRelay::unannounce(const TrackNamespace& trackNamespace, AnnounceNode*) {
  XLOG(DBG1) << __func__ << " ns=" << trackNamespace;
  // Node would be useful if there were back links
  auto nodePtr = findNamespaceNode(trackNamespace);
  if (!nodePtr) {
    // Node was already pruned, nothing to do, maybe app called unannouce twice?
    XLOG(DBG1) << "Node already pruned for ns=" << trackNamespace;
    return;
  }

  // Track if node had local content before modification
  bool hadLocalContent = nodePtr->hasLocalSessions();

  auto session = MoQSession::getRequestSession();

  // Only allow unannounce if there is an owner and the caller is that owner
  if (nodePtr->sourceSession == nullptr || nodePtr->sourceSession != session) {
    XLOG(DBG1) << "Ignoring unannounce for ns=" << trackNamespace
               << " (no owner or non-owner session)";
    return;
  }

  nodePtr->sourceSession = nullptr;
  nodePtr->announceCallback.reset();
  for (auto& announcement : nodePtr->announcements) {
    auto exec = announcement.first->getExecutor();
    exec->add([announceHandle = announcement.second] {
      announceHandle->unannounce();
    });
  }
  nodePtr->announcements.clear();

  // Prune if node became empty and has a parent
  if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
      !trackNamespace.trackNamespace.empty()) {
    nodePtr->parent_->tryPruneChild(trackNamespace.trackNamespace.back());
  }
}

void MoQRelay::onPublishDone(const FullTrackName& ftn) {
  XLOG(DBG1) << __func__ << " ftn=" << ftn;

  auto it = subscriptions_.find(ftn);
  if (it != subscriptions_.end()) {
    if (it->second.isPublish) {
      // Remove from publishes map
      auto nodePtr = findNamespaceNode(ftn.trackNamespace);
      if (nodePtr) {
        bool hadLocalContent = nodePtr->hasLocalSessions();
        nodePtr->publishes.erase(ftn.trackName);

        // Prune if node became empty and has a parent
        if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
            !ftn.trackNamespace.trackNamespace.empty()) {
          nodePtr->parent_->tryPruneChild(
              ftn.trackNamespace.trackNamespace.back());
        }
      }
    }

    // Clear the handle and upstream - this signals publisher is done
    // Clearing upstream is important to break circular reference:
    // session holds FilterConsumer, relay holds session in upstream
    it->second.handle.reset();
    it->second.upstream.reset();

    // If forwarder has no subscribers, clean up immediately
    // Otherwise onEmpty will be called when last subscriber leaves
    if (it->second.forwarder->empty()) {
      XLOG(DBG1) << "Publisher terminated with no subscribers, cleaning up "
                 << ftn;
      subscriptions_.erase(it);
    }
  }
}

Subscriber::PublishResult MoQRelay::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  XLOG(DBG1) << __func__ << " ftn=" << pub.fullTrackName;
  XCHECK(handle) << "Publish handle cannot be null";
  if (!pub.fullTrackName.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID, PublishErrorCode::UNINTERESTED, "bad namespace"});
  }

  if (pub.fullTrackName.trackNamespace.empty()) {
    return folly::makeUnexpected(PublishError(
        {pub.requestID,
         PublishErrorCode::INTERNAL_ERROR,
         "namespace required"}));
  }

  // Find All Nodes that SubscribeAnnounced to this namespace (including prefix
  // ns)
  std::vector<std::pair<std::shared_ptr<MoQSession>, bool>> sessions = {};
  auto nodePtr = findNamespaceNode(
      pub.fullTrackName.trackNamespace,
      /*createMissingNodes=*/true,
      MatchType::Exact,
      &sessions);
  // Extract session pointers with their forward preferences from the map
  for (const auto& [sessionPtr, forward] : nodePtr->sessions) {
    sessions.emplace_back(sessionPtr, forward);
  }

  auto session = MoQSession::getRequestSession();
  bool wasEmpty = !nodePtr->hasLocalSessions();

  std::shared_ptr<MoQForwarder> migratedForwarder;
  auto it = subscriptions_.find(pub.fullTrackName);
  if (it != subscriptions_.end()) {
    // Check if same session is transitioning from SUBSCRIBE-mode
    // (created by a downstream subscriber like moq_ingress) to
    // PUBLISH-mode (browser's PUBLISH arriving after camera init).
    // This is NOT a multipublisher collision — it's the same publisher
    // upgrading. Migrate the existing forwarder (with its subscribers)
    // instead of tearing it down, which would kill active subscriptions
    // with reset_stream error=3 (CANCELLED).
    if (it->second.upstream.get() == session.get()) {
      XLOG(INFO) << "[JEKET] Same-session SUBSCRIBE→PUBLISH migration for "
                 << pub.fullTrackName << " — preserving "
                 << it->second.forwarder->numForwardingSubscribers()
                 << " subscriber(s)";
      migratedForwarder = it->second.forwarder;
      it->second.handle->unsubscribe();  // cancel now-redundant upstream subscribe
    } else {
      // Different publisher — original teardown logic
      XLOG(DBG1) << "New publisher for existing subscription (different session)";
      nodePtr->publishes.erase(pub.fullTrackName.trackName);
      it->second.handle->unsubscribe();
      it->second.forwarder->subscribeDone(
          {RequestID(0),
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           0, // filled in by session
           "upstream disconnect"});
    }
    XLOG(DBG4) << "Erasing subscription to " << it->first;
    subscriptions_.erase(it);
  }
  // JEKET fork: defense in depth for publisher reconnect. The primary
  // cleanup path is onPublishDone()/onEmpty() which should erase the
  // stale publishes entry before we get here. But if anything leaks past
  // those paths, the upstream XCHECK(Duplicate publish) crashes the
  // whole relay process. Replace is safer and idempotent — overwrite
  // the stale session pointer with the new one. See Jeket-com/JSS#30.
  auto res = nodePtr->publishes.emplace(pub.fullTrackName.trackName, session);
  if (!res.second) {
    XLOG(WARNING) << "Duplicate publish for " << pub.fullTrackName
                  << " — replacing stale entry (reconnect-race cleanup)";
    res.first->second = session;
  }

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->hasLocalSessions() && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }

  // Create Forwarder for this publish — or reuse migrated one from
  // a same-session SUBSCRIBE→PUBLISH transition (preserves subscribers)
  auto forwarder = migratedForwarder
      ? migratedForwarder
      : std::make_shared<MoQForwarder>(pub.fullTrackName, folly::none);

  // Set Forwarder Params
  forwarder->setGroupOrder(pub.groupOrder);

  // Extract delivery timeout from publish request params and store in forwarder
  auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
      pub.params, session->getNegotiatedVersion().value());
  if (deliveryTimeout && *deliveryTimeout > 0) {
    forwarder->setDeliveryTimeout(*deliveryTimeout);
  }

  // JEKET fork: on duplicate, std::unordered_map::emplace with
  // piecewise_construct does NOT replace the existing value — it returns
  // the iterator to the EXISTING entry and res.second=false. If we only
  // rewrote rsub.handle/requestID/isPublish in that case (the old behavior
  // of this method), rsub.forwarder and rsub.upstream would still point
  // at the DEAD previous publisher session's state. Subsequent browser
  // subscribes find the corrupted entry → tries to wire into the old
  // forwarder (which has no data flow) → falls through to the upstream
  // path → routes the subscribe to the new publisher as if it were a
  // pull subscriber → publisher rejects with "PUBLISH-mode: rejected".
  //
  // Fix: if an entry already exists for this FTN, erase it first so the
  // emplace below creates a clean entry with the NEW forwarder + NEW
  // session. The primary cleanup path in onEmpty() should usually remove
  // it before we reach this branch, but on specific reconnect races the
  // old entry can linger. See Jeket-com/JSS#55.
  {
    auto existingIt = subscriptions_.find(pub.fullTrackName);
    if (existingIt != subscriptions_.end()) {
      XLOG(WARNING) << "JSS#55: subscriptions_ duplicate for "
                    << pub.fullTrackName
                    << " on publisher reconnect — erasing stale entry";
      subscriptions_.erase(existingIt);
    }
  }
  auto subRes = subscriptions_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(pub.fullTrackName),
      std::forward_as_tuple(forwarder, session));
  XCHECK(subRes.second) << "subscriptions_.emplace still duplicate after erase — bug";
  auto& rsub = subRes.first->second;
  rsub.promise.setValue(folly::unit);
  rsub.requestID = pub.requestID;
  rsub.handle = std::move(handle);
  rsub.isPublish = true;

  uint64_t nSubscribers = 0;
  for (auto& [outSession, forward] : sessions) {
    if (outSession != session) {
      nSubscribers++;
      auto exec = outSession->getExecutor();
      co_withExecutor(
          exec, publishToSession(outSession, forwarder, pub, forward))
          .start();
    }
  }
  (void)nSubscribers; // unused — see note below
  forwarder->setCallback(shared_from_this());

  // Jeket fork: publisher is always allowed to push. The upstream relay
  // gates PUBLISH forwarding on the presence of a downstream SubscribeAnnounces
  // subscriber (nSubscribers > 0), which creates a chicken-and-egg deadlock
  // for live egress: Cloudflare only subscribes when a viewer arrives, the
  // viewer can't play without cached data, and the relay can't cache without
  // the publisher pushing. We break the deadlock by accepting push data
  // unconditionally — the forwarder + MoQCache buffer recent groups for
  // late-joining subscribers. SUBSCRIBE_UPDATE from arriving/leaving
  // subscribers can still tune per-subgroup delivery; this only changes the
  // initial PublishOk gate.
  //
  // Use getSubscribeWriteback() to wire the publisher's push path through
  // MoQCache (if enabled) and TerminationFilter, mirroring the upstream-
  // SUBSCRIBE path (see the subscribe() branch). Without this, PUBLISHed
  // data would bypass the cache and be dropped for any subscriber that
  // arrives after the group they care about was pushed.
  //
  // JEKET fork: clear any stale cached objects for this track before wiring
  // the new writeback. When a publisher restarts (e.g. moq-egress pod
  // bounce), its new session starts fresh group/object numbering. Without
  // this, the cache still holds the OLD completed CacheEntry at group=1
  // obj=0 and MoQCache::cacheObject rejects the new data with "Payload
  // mismatch; objID=0", tearing down the incoming subgroup stream.
  if (cache_) {
    cache_->clearTrack(pub.fullTrackName);
  }
  auto publishConsumer = getSubscribeWriteback(pub.fullTrackName, forwarder);

  return PublishConsumerAndReplyTask{
      publishConsumer,
      folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(PublishOk{
          pub.requestID,
          /*forward=*/true,
          kDefaultPriority,
          pub.groupOrder,
          LocationType::AbsoluteRange,
          kLocationMin,
          kLocationMax.group,
          {}})};
}

folly::coro::Task<void> MoQRelay::publishToSession(
    std::shared_ptr<MoQSession> session,
    std::shared_ptr<MoQForwarder> forwarder,
    PublishRequest pub,
    bool forward) {
  pub.forward = forward;
  auto subscriber = forwarder->addSubscriber(session, pub);
  if (!subscriber) {
    XLOG(ERR) << "Subscribe failed: addSubscriber returned null for "
              << forwarder->fullTrackName() << " reqID=" << pub.requestID;
    co_return;
  }
  XLOG(DBG4) << "added subscriber for ftn=" << pub.fullTrackName;
  auto guard = folly::makeGuard([subscriber] { subscriber->unsubscribe(); });
  if (pub.largest) {
    subscriber->updateLargest(*pub.largest);
  }
  subscriber->setPublisherGroupOrder(pub.groupOrder);

  auto pubInitial = session->publish(pub, subscriber);
  if (pubInitial.hasError()) {
    XLOG(ERR) << "Publish failed err=" << pubInitial.error().reasonPhrase;
    co_return;
  }
  subscriber->trackConsumer = std::move(pubInitial->consumer);
  auto pubResult = co_await co_awaitTry(std::move(pubInitial->reply));
  if (pubResult.hasException()) {
    XLOG(ERR) << "Publish failed err=" << pubResult.exception().what();
    co_return;
  }
  if (pubResult.value().hasError()) {
    XLOG(ERR) << "Publish failed err="
              << pubResult.value().error().reasonPhrase;
    co_return;
  }
  guard.dismiss();
  XLOG(DBG1) << "Publish OK sess=" << session.get();
  auto& pubOk = pubResult.value().value();
  folly::Optional<AbsoluteLocation> end;
  if (pubOk.endGroup) {
    end = AbsoluteLocation{*pubOk.endGroup, 0};
  }
  subscriber->range =
      toSubscribeRange(pubOk.start, end, pubOk.locType, forwarder->largest());
  subscriber->shouldForward = pubOk.forward;
}

class MoQRelay::AnnouncesSubscription
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  AnnouncesSubscription(
      std::shared_ptr<MoQRelay> relay,
      std::shared_ptr<MoQSession> session,
      SubscribeAnnouncesOk ok,
      TrackNamespace trackNamespacePrefix)
      : Publisher::SubscribeAnnouncesHandle(std::move(ok)),
        relay_(std::move(relay)),
        session_(std::move(session)),
        trackNamespacePrefix_(std::move(trackNamespacePrefix)) {}

  void unsubscribeAnnounces() override {
    if (relay_) {
      relay_->unsubscribeAnnounces(trackNamespacePrefix_, std::move(session_));
      relay_.reset();
    }
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
  std::shared_ptr<MoQSession> session_;
  TrackNamespace trackNamespacePrefix_;
};

// Filter TrackConsumer that intercepts subscribeDone to clean up relay state
class MoQRelay::TerminationFilter : public TrackConsumerFilter {
 public:
  TerminationFilter(
      std::shared_ptr<MoQRelay> relay,
      FullTrackName ftn,
      std::shared_ptr<TrackConsumer> downstream)
      : TrackConsumerFilter(std::move(downstream)),
        relay_(std::move(relay)),
        ftn_(std::move(ftn)) {}

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    // Notify relay that publisher is done - this will:
    // 1. Remove from nodePtr->publishes
    // 2. Clear subscription.handle
    if (relay_) {
      relay_->onPublishDone(ftn_);
    }
    // Change the downstream code to something like "upstream ended"?
    return TrackConsumerFilter::subscribeDone(std::move(subDone));
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
  FullTrackName ftn_;
};

std::shared_ptr<TrackConsumer> MoQRelay::getSubscribeWriteback(
    const FullTrackName& ftn,
    std::shared_ptr<TrackConsumer> consumer) {
  auto baseConsumer = cache_
      ? cache_->getSubscribeWriteback(ftn, std::move(consumer))
      : std::move(consumer);
  auto filterConsumer = std::make_shared<TerminationFilter>(
      shared_from_this(), ftn, std::move(baseConsumer));
  return std::static_pointer_cast<TrackConsumer>(filterConsumer);
}

folly::coro::Task<Publisher::SubscribeAnnouncesResult>
MoQRelay::subscribeAnnounces(SubscribeAnnounces subNs) {
  XLOG(DBG1) << __func__ << " nsp=" << subNs.trackNamespacePrefix;
  // check auth
  if (subNs.trackNamespacePrefix.empty()) {
    co_return folly::makeUnexpected(
        SubscribeAnnouncesError{
            subNs.requestID,
            SubscribeAnnouncesErrorCode::NAMESPACE_PREFIX_UNKNOWN,
            "empty"});
  }
  auto session = MoQSession::getRequestSession();
  auto nodePtr = findNamespaceNode(
      subNs.trackNamespacePrefix, /*createMissingNodes=*/true);

  // Check if this is the first session subscriber for this node
  bool wasEmpty = !nodePtr->hasLocalSessions();
  nodePtr->sessions.emplace(session, subNs.forward);

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->hasLocalSessions() && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }

  // Find all nested Announcements/Publishes and forward
  std::deque<std::tuple<TrackNamespace, std::shared_ptr<AnnounceNode>>> nodes{
      {subNs.trackNamespacePrefix, nodePtr}};
  auto exec = session->getExecutor();
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    if (nodePtr->sourceSession && nodePtr->sourceSession != session) {
      // TODO: Auth/params
      co_withExecutor(
          exec,
          announceToSession(session, {subNs.requestID, prefix, {}}, nodePtr))
          .start();
    }
    PublishRequest pub{
        0,
        FullTrackName{prefix, ""},
        TrackAlias(0), // filled in by library
        GroupOrder::Default,
        folly::none,
        /*forward=*/false,
        {}};
    for (auto& publishEntry : nodePtr->publishes) {
      auto& publishSession = publishEntry.second;
      pub.fullTrackName.trackName = publishEntry.first;
      auto subscriptionIt = subscriptions_.find(pub.fullTrackName);
      if (subscriptionIt == subscriptions_.end()) {
        XLOG(ERR) << "Invalid state, no subscription for publish ftn="
                  << pub.fullTrackName;
        continue;
      }
      auto& forwarder = subscriptionIt->second.forwarder;
      if (forwarder->empty()) {
        // Use forward value from this namespace subscription
        co_withExecutor(
            exec,
            doSubscribeUpdate(subscriptionIt->second.handle, subNs.forward))
            .start();
      }
      pub.groupOrder = forwarder->groupOrder();
      pub.largest = forwarder->largest();
      if (publishSession != session) {
        co_withExecutor(
            exec, publishToSession(session, forwarder, pub, subNs.forward))
            .start();
      }
    }
    for (auto& nextNodeIt : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNodeIt.first);
      nodes.emplace_back(std::forward_as_tuple(nodePrefix, nextNodeIt.second));
    }
  }
  co_return std::make_shared<AnnouncesSubscription>(
      shared_from_this(),
      std::move(session),
      SubscribeAnnouncesOk{subNs.requestID, {}},
      subNs.trackNamespacePrefix);
}

void MoQRelay::unsubscribeAnnounces(
    const TrackNamespace& trackNamespacePrefix,
    std::shared_ptr<MoQSession> session) {
  XLOG(DBG1) << __func__ << " nsp=" << trackNamespacePrefix;
  auto nodePtr = findNamespaceNode(trackNamespacePrefix);
  if (!nodePtr) {
    // TODO: maybe error?
    return;
  }

  // Track if node had local content before modification
  bool hadLocalContent = nodePtr->hasLocalSessions();

  auto it = nodePtr->sessions.find(session);
  if (it != nodePtr->sessions.end()) {
    nodePtr->sessions.erase(it);

    // Prune if node became empty and has a parent
    if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
        !trackNamespacePrefix.trackNamespace.empty()) {
      nodePtr->parent_->tryPruneChild(
          trackNamespacePrefix.trackNamespace.back());
    }
    return;
  }
  // TODO: error?
  XLOG(DBG1) << "Namespace prefix was not subscribed by this session";
}

std::shared_ptr<MoQSession> MoQRelay::findAnnounceSession(
    const TrackNamespace& ns) {
  /*
   * This function is called from subscribe() and fetch().
   * We use MatchType::Prefix here because the relay routes SUBSCRIBE and FETCH
   * to the publisher who announced the closest matching broader namespace, not
   * necessarily the exact match.
   */
  auto nodePtr =
      findNamespaceNode(ns, /*createMissingNodes=*/false, MatchType::Prefix);
  if (!nodePtr) {
    return nullptr;
  }
  return nodePtr->sourceSession;
}

MoQRelay::PublishState MoQRelay::findPublishState(const FullTrackName& ftn) {
  PublishState state;
  auto nodePtr = findNamespaceNode(
      ftn.trackNamespace, /*createMissingNodes=*/false, MatchType::Exact);

  if (!nodePtr) {
    // Node doesn't exist - tree was properly pruned
    return state;
  }

  state.nodeExists = true;

  auto it = nodePtr->publishes.find(ftn.trackName);
  if (it != nodePtr->publishes.end()) {
    state.session = it->second;
  }

  return state;
}

folly::coro::Task<Publisher::SubscribeResult> MoQRelay::subscribe(
    SubscribeRequest subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  auto session = MoQSession::getRequestSession();
  auto subscriptionIt = subscriptions_.find(subReq.fullTrackName);
  if (subscriptionIt == subscriptions_.end()) {
    // first subscriber

    // check auth
    // get trackNamespace
    if (subReq.fullTrackName.trackNamespace.empty()) {
      // message error?
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           SubscribeErrorCode::TRACK_NOT_EXIST,
           "namespace required"}));
    }
    auto upstreamSession =
        findAnnounceSession(subReq.fullTrackName.trackNamespace);
    if (!upstreamSession) {
      // no such namespace has been announced
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           SubscribeErrorCode::TRACK_NOT_EXIST,
           "no such namespace or track"}));
    }
    subReq.priority = kDefaultUpstreamPriority;
    subReq.groupOrder = GroupOrder::Default;
    // We only subscribe upstream with LargestObject. This is to satisfy other
    // subscribers that join with narrower filters
    subReq.locType = LocationType::LargestObject;
    auto forwarder =
        std::make_shared<MoQForwarder>(subReq.fullTrackName, folly::none);
    forwarder->setCallback(shared_from_this());
    auto emplaceRes = subscriptions_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(subReq.fullTrackName),
        std::forward_as_tuple(forwarder, upstreamSession));
    // The iterator returned from emplace does not survive across coroutine
    // resumption, so both the guard and updating the RelaySubscription below
    // require another lookup in the subscriptions_ map.
    auto g = folly::makeGuard([this, trackName = subReq.fullTrackName] {
      auto it = subscriptions_.find(trackName);
      if (it != subscriptions_.end()) {
        it->second.promise.setException(std::runtime_error("failed"));
        XLOG(DBG4) << "Erasing subscription to " << it->first;
        subscriptions_.erase(it);
      }
    });
    // Add subscriber first in case objects come before subscribe OK.
    auto sessionVersion = session->getNegotiatedVersion();
    auto subscriber = forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    if (!subscriber) {
      XLOG(ERR) << "addSubscriber returned null (draining?) for "
                << subReq.fullTrackName << " reqID=" << subReq.requestID;
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::INTERNAL_ERROR,
              "failed to add subscriber"});
    }
    XLOG(DBG4) << "added subscriber for ftn=" << subReq.fullTrackName;
    // As per the spec, we must set forward = true in the subscribe request
    // to the upstream.
    // But should we if this is forward=0?
    subReq.forward = forwarder->numForwardingSubscribers() > 0;

    emplaceRes.first->second.requestID = upstreamSession->peekNextRequestID();
    auto subRes = co_await upstreamSession->subscribe(
        subReq, getSubscribeWriteback(subReq.fullTrackName, forwarder));
    if (subRes.hasError()) {
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           subRes.error().errorCode,
           folly::to<std::string>(
               "upstream subscribe failed: ", subRes.error().reasonPhrase)}));
    }
    // is it more correct to co_await folly::coro::co_safe_point here?
    g.dismiss();
    auto largest = subRes.value()->subscribeOk().largest;
    if (largest) {
      forwarder->updateLargest(largest->group, largest->object);
      subscriber->updateLargest(*largest);
    }
    auto pubGroupOrder = subRes.value()->subscribeOk().groupOrder;
    forwarder->setGroupOrder(pubGroupOrder);

    // Store upstream delivery timeout in forwarder
    auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
        subRes.value()->subscribeOk().params, sessionVersion.value());

    // Add delivery timeout to downstream subscriber explicitly as this is the
    // first subscriber. Forwarder can add it to subsequent subscribers
    if (deliveryTimeout && *deliveryTimeout > 0) {
      forwarder->setDeliveryTimeout(*deliveryTimeout);
      subscriber->setParam(
          {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
           *deliveryTimeout});
    }

    subscriber->setPublisherGroupOrder(pubGroupOrder);
    auto it = subscriptions_.find(subReq.fullTrackName);
    // There are cases that remove the subscription like failing to
    // publish a datagram that was received before the subscribeOK
    // and then gets flushed
    if (it == subscriptions_.end()) {
      XLOG(ERR) << "Subscription is GONE, returning exception";
      co_yield folly::coro::co_error(
          std::runtime_error("subscription is gone"));
    }
    auto& rsub = it->second;
    rsub.requestID = subRes.value()->subscribeOk().requestID;
    rsub.handle = std::move(subRes.value());
    rsub.promise.setValue(folly::unit);
    co_return subscriber;
  } else {
    if (!subscriptionIt->second.promise.isFulfilled()) {
      // this will throw if the dependent subscribe failed, which is good
      // because subscriptionIt will be invalid
      co_await subscriptionIt->second.promise.getFuture();
    }
    auto& forwarder = subscriptionIt->second.forwarder;
    if (forwarder->largest() && subReq.locType == LocationType::AbsoluteRange &&
        subReq.endGroup < forwarder->largest()->group) {
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::INVALID_RANGE,
              "Range in the past, use FETCH"});
      // start may be in the past, it will get adjusted forward to largest
    }
    bool forwarding =
        subscriptionIt->second.forwarder->numForwardingSubscribers() > 0;
    auto subscriber = subscriptionIt->second.forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    if (!subscriber) {
      XLOG(ERR) << "addSubscriber returned null (draining?) for "
                << subReq.fullTrackName << " reqID=" << subReq.requestID;
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::INTERNAL_ERROR,
              "failed to add subscriber"});
    }
    XLOG(DBG4) << "added subscriber for ftn=" << subReq.fullTrackName;
    if (!forwarding &&
        subscriptionIt->second.forwarder->numForwardingSubscribers() > 0) {
      auto exec = subscriptionIt->second.upstream->getExecutor();
      co_withExecutor(
          exec,
          doSubscribeUpdate(subscriptionIt->second.handle, /*forward=*/true))
          .start();
    }
    co_return subscriber;
  }
}

folly::coro::Task<Publisher::FetchResult> MoQRelay::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  auto session = MoQSession::getRequestSession();

  // check auth
  // get trackNamespace
  if (fetch.fullTrackName.trackNamespace.empty()) {
    co_return folly::makeUnexpected(FetchError(
        {fetch.requestID,
         FetchErrorCode::TRACK_NOT_EXIST,
         "namespace required"}));
  }

  auto [standalone, joining] = fetchType(fetch);
  if (joining) {
    auto subscriptionIt = subscriptions_.find(fetch.fullTrackName);
    if (subscriptionIt == subscriptions_.end()) {
      XLOG(ERR) << "No subscription for joining fetch";
      // message error
      co_return folly::makeUnexpected(FetchError(
          {fetch.requestID,
           FetchErrorCode::TRACK_NOT_EXIST,
           "No subscription for joining fetch"}));
    } else if (subscriptionIt->second.promise.isFulfilled()) {
      auto res = subscriptionIt->second.forwarder->resolveJoiningFetch(
          session, *joining);
      if (res.hasError()) {
        co_return folly::makeUnexpected(res.error());
      }
      fetch.args = StandaloneFetch(res.value().start, res.value().end);
      joining = nullptr;
    } else {
      // Upstream is resolving the subscribe, forward joining fetch
      joining->joiningRequestID = subscriptionIt->second.requestID;
    }
  }

  auto upstreamSession =
      findAnnounceSession(fetch.fullTrackName.trackNamespace);
  if (!upstreamSession) {
    // Attempt to find matching upstream subscription (from publish)
    auto subscriptionIt = subscriptions_.find(fetch.fullTrackName);
    if (subscriptionIt != subscriptions_.end()) {
      upstreamSession = subscriptionIt->second.upstream;
    } else {
      // no such namespace has been announced
      co_return folly::makeUnexpected(FetchError(
          {fetch.requestID,
           FetchErrorCode::TRACK_NOT_EXIST,
           "no such namespace"}));
    }
  }
  if (session.get() == upstreamSession.get()) {
    co_return folly::makeUnexpected(FetchError(
        {fetch.requestID, FetchErrorCode::INTERNAL_ERROR, "self fetch"}));
  }
  fetch.priority = kDefaultUpstreamPriority;
  if (!cache_ || joining) {
    // We can't use the cache on an unresolved joining fetch - we don't know
    // which objects are being requested.  However, once we have that resolved,
    // we SHOULD be able to serve from cache.
    if (standalone) {
      XLOG(DBG1) << "Upstream fetch {" << standalone->start.group << ","
                 << standalone->start.object << "}.." << standalone->end.group
                 << "," << standalone->end.object << "}";
    }
    co_return co_await upstreamSession->fetch(fetch, std::move(consumer));
  }
  co_return co_await cache_->fetch(
      fetch, std::move(consumer), std::move(upstreamSession));
}

void MoQRelay::onEmpty(MoQForwarder* forwarder) {
  auto subscriptionIt = subscriptions_.find(forwarder->fullTrackName());
  if (subscriptionIt == subscriptions_.end()) {
    return;
  }
  auto& subscription = subscriptionIt->second;

  if (!subscription.handle) {
    // Handle is null - publisher terminated via FilterConsumer
    XLOG(INFO) << "Publisher terminated for " << subscriptionIt->first;
    // JEKET fork: the publisher's entry in nodePtr->publishes must be
    // erased alongside subscriptions_, otherwise a reconnect triggers
    // `XCHECK(Duplicate publish)` in MoQRelay::publish() when the new
    // session tries to emplace into the still-populated publishes map.
    // onPublishDone() takes care of this on a clean close, but a
    // publisher that terminates mid-stream reaches us here instead.
    // See Jeket-com/JSS#30.
    const auto& ftn = subscriptionIt->first;
    if (subscription.isPublish) {
      auto nodePtr = findNamespaceNode(ftn.trackNamespace);
      if (nodePtr) {
        bool hadLocalContent = nodePtr->hasLocalSessions();
        nodePtr->publishes.erase(ftn.trackName);
        if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
            !ftn.trackNamespace.trackNamespace.empty()) {
          nodePtr->parent_->tryPruneChild(
              ftn.trackNamespace.trackNamespace.back());
        }
      }
    }
    subscriptions_.erase(subscriptionIt);
    return;
  }

  // Handle exists - just last subscriber left
  XLOG(INFO) << "Last subscriber removed for " << subscriptionIt->first;
  if (subscription.isPublish) {
    // JEKET fork: do NOT send SUBSCRIBE_UPDATE forward=false to the
    // publisher when the last downstream subscriber leaves. Upstream
    // moxygen's behavior pauses the publisher, which starves the MoQCache
    // writeback of data — so a late-joining viewer finds an empty cache and
    // can't play until the next subscriber-triggered forward=true cycle.
    //
    // This is the same rationale as the publish() fork fix: with MoQCache
    // enabled on the relay, the publisher feeds the cache continuously and
    // the cache serves late joiners. Pausing the publisher on "0
    // subscribers" re-creates the chicken-and-egg deadlock we fixed in
    // Jeket-com/JSS#24.
    //
    // Keep the subscription alive; keep forward=true. The publisher keeps
    // pushing; MoQCache evicts old groups naturally; late subscribers pick
    // up the most recent cached groups on subscribe.
    XLOG(DBG1) << "isPublish: keeping publisher forward=true for cache warmth";
  } else {
    subscription.handle->unsubscribe();
    XLOG(DBG4) << "Erasing subscription to " << subscriptionIt->first;
    subscriptions_.erase(subscriptionIt);
  }
}

void MoQRelay::forwardChanged(MoQForwarder* forwarder) {
  auto subscriptionIt = subscriptions_.find(forwarder->fullTrackName());
  if (subscriptionIt == subscriptions_.end()) {
    return;
  }
  auto& subscription = subscriptionIt->second;
  if (!subscription.promise.isFulfilled()) {
    // Ignore: it's the first subscriber, forward update not needed
    return;
  }
  // JEKET fork: for isPublish subscriptions, never propagate a forward=false
  // flip back to the publisher (see onEmpty comment above — we keep the
  // MoQCache warm even when 0 live subscribers are present). Upstream-
  // SUBSCRIBE paths still honor the subscriber count.
  if (subscription.isPublish) {
    XLOG(DBG1) << "forwardChanged: isPublish — skipping forward update for "
               << subscriptionIt->first;
    return;
  }
  XLOG(INFO) << "Updating forward for " << subscriptionIt->first
             << " numForwardingSubs=" << forwarder->numForwardingSubscribers();

  auto exec = subscription.upstream->getExecutor();
  co_withExecutor(
      exec,
      doSubscribeUpdate(
          subscription.handle,
          /*forward=*/forwarder->numForwardingSubscribers() > 0))
      .start();
}

} // namespace moxygen
