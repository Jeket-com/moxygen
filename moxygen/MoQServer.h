/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/httpserver/samples/hq/HQServer.h>
#include <moxygen/MoQServerBase.h>

#include <folly/init/Init.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/io/async/EventBaseManager.h>

#include <utility>

#include "moxygen/MoQSession.h"

namespace moxygen {

const std::string kDefaultFilePath =
    "ti/experimental/moxygen/moqtest/mlog_server.txt";

class MoQServer : public MoQServerBase {
 public:
  MoQServer(
      std::string cert,
      std::string key,
      std::string endpoint,
      folly::Optional<quic::TransportSettings> transportSettings = folly::none);

  MoQServer(
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      std::string endpoint,
      folly::Optional<quic::TransportSettings> transportSettings = folly::none);

  void start(const folly::SocketAddress& addr) override {
    start(addr, {});
  }

  void start(
      const folly::SocketAddress& addr,
      std::vector<folly::EventBase*> evbs);

  MoQServer(const MoQServer&) = delete;
  MoQServer(MoQServer&&) = delete;
  MoQServer& operator=(const MoQServer&) = delete;
  MoQServer& operator=(MoQServer&&) = delete;
  ~MoQServer() override = default;

  std::vector<folly::EventBase*> getWorkerEvbs() const noexcept {
    return hqServer_->getWorkerEvbs();
  }

  // QUIC stats factory setter
  void setQuicStatsFactory(
      std::unique_ptr<quic::QuicTransportStatsCallbackFactory> factory);

  void stop() override;

  // Takeover runtime wrapper methods - forward to underlying QuicServer
  // Takeover part 1: Methods called on the old instance.
  void allowBeingTakenOver(const folly::SocketAddress& addr);
  int getTakeoverHandlerSocketFD() const;
  std::vector<int> getAllListeningSocketFDs() const;

  // Takeover part 2: Methods called during the initialization of the new
  // process.
  void setListeningFDs(const std::vector<int>& fds);
  void setProcessId(quic::ProcessId pid);
  void setHostId(uint32_t hostId);
  void setConnectionIdVersion(quic::ConnectionIdVersion version);
  void waitUntilInitialized();

  // Takeover part 3: Methods called during the packet forwarding setup.
  quic::ProcessId getProcessId() const;
  quic::TakeoverProtocolVersion getTakeoverProtocolVersion() const;
  void startPacketForwarding(const folly::SocketAddress& addr);

  // Takeover part 4: Methods called on the old instance to wind down.
  void rejectNewConnections(std::function<bool()> rejectFn);
  void pauseRead();

  void setFizzContext(
      std::shared_ptr<const fizz::server::FizzServerContext> ctx);

  void setFizzContext(
      folly::EventBase* evb,
      std::shared_ptr<const fizz::server::FizzServerContext> ctx);

 protected:
  // Register ALPN handlers for direct QUIC connections (internal use)
  void registerAlpnHandler(const std::vector<std::string>& alpns);

 private:
  void createMoQQuicSession(std::shared_ptr<quic::QuicSocket> quicSocket);

  std::shared_ptr<MoQExecutor> getOrCreateExecutor(folly::EventBase* evb);

  class Handler : public proxygen::HTTPTransactionHandler {
   public:
    explicit Handler(MoQServer& server) : server_(server) {}

    void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
      txn_ = txn;
    }
    void detachTransaction() noexcept override {
      txn_ = nullptr;
      delete this;
    }
    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> req) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf>) noexcept override {}
    void onTrailers(std::unique_ptr<proxygen::HTTPHeaders>) noexcept override {}
    void onEOM() noexcept override {
      XLOG(DBG1) << "WebTransport session terminated";
      onSessionEnd(folly::none);
      if (!txn_->isEgressEOMSeen()) {
        txn_->sendEOM();
      }
    }
    void onUpgrade(proxygen::UpgradeProtocol) noexcept override {}
    void onError(const proxygen::HTTPException& error) noexcept override {
      // JEKET fork: swallow the 60s "ingress timeout" fake exception.
      // WebTransport CONNECT streams have no ingress body by design — all
      // MoQ data flows on SEPARATE unidirectional streams, not on stream 0.
      // But proxygen's per-stream ingress idle timer still fires on the
      // CONNECT stream every ~60s and tears down the WebTransport session.
      // The params_.txnTimeout = 24h override we set in the server
      // constructor isn't propagating to the HTTPTransaction idle timer
      // for reasons we haven't diagnosed (see Jeket-com/JSS#74).
      //
      // Filter the error message specifically for "ingress timeout"; any
      // other HTTPException still tears down the session as normal.
      // If proxygen force-detaches the transaction after onError returns,
      // we'll see it in the next restart and know this workaround isn't
      // sufficient. But for the demo window we need WebTransport sessions
      // to outlive 60 seconds.
      const auto msg = folly::exceptionStr(error).toStdString();
      if (msg.find("ingress timeout") != std::string::npos) {
        XLOG(WARNING) << "[JEKET] Ignoring proxygen ingress timeout — "
                      << "WebTransport CONNECT stream is idle by design. "
                      << "Session stays alive. " << msg;
        return;  // do NOT tear down the session
      }
      XLOG(ERR) << msg;
      onSessionEnd(proxygen::WebTransport::kInternalError);
    }
    void onEgressPaused() noexcept override {}
    void onEgressResumed() noexcept override {}
    void onWebTransportBidiStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::BidiStreamHandle handle) noexcept override {
      clientSession_->onNewBidiStream(handle);
    }
    void onWebTransportUniStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::StreamReadHandle* handle) noexcept override {
      clientSession_->onNewUniStream(handle);
    }
    void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept override {
      clientSession_->onDatagram(std::move(datagram));
    }

   private:
    void onSessionEnd(folly::Optional<uint32_t> err) {
      if (clientSession_) {
        clientSession_->onSessionEnd(std::move(err));
        clientSession_.reset();
      }
    }
    MoQServer& server_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::shared_ptr<MoQSession> clientSession_;
  };

  std::string cert_;
  std::string key_;
  quic::samples::HQServerParams params_;
  std::shared_ptr<const fizz::server::FizzServerContext> fizzContext_;
  std::unique_ptr<quic::samples::HQServerTransportFactory> factory_;
  std::unique_ptr<quic::samples::HQServer> hqServer_;
  folly::EventBaseLocal<std::shared_ptr<MoQExecutor>> executorLocal_;

  friend class Handler;
};
} // namespace moxygen
