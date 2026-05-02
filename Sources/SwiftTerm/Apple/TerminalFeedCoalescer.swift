//
//  TerminalFeedCoalescer.swift
//  SwiftTerm
//
//  Coalesces high-frequency terminal output before feeding TerminalView.
//

#if os(macOS) || os(iOS) || os(visionOS)
import Foundation

/// Describes how ``TerminalFeedCoalescer`` handles output when producers are
/// faster than the UI can safely consume.
public enum TerminalFeedBackpressurePolicy {
    /// Keep the newest bytes and discard the oldest pending bytes.
    ///
    /// This is the best default for live log viewers because the newest output is
    /// usually the most useful output when the UI is falling behind.
    case keepNewest

    /// Keep the oldest pending bytes and discard newly appended overflow bytes.
    ///
    /// This preserves terminal stream continuity better than ``keepNewest`` but
    /// may make a live log view lag behind a noisy producer.
    case keepOldest

    /// Never discard pending bytes.
    ///
    /// Use this only when the producer is already bounded. A bursty or unbounded
    /// stream can grow memory without limit.
    case unbounded
}

/// Batches high-frequency terminal output before feeding it into a ``TerminalView``.
///
/// SwiftTerm's normal ``TerminalView/feed(byteArray:)`` and ``TerminalView/feed(text:)``
/// methods intentionally parse and update the terminal immediately. That is the right
/// behavior for interactive terminal sessions, but it can overload the main thread when
/// callers forward many small network, SSH, process, or log chunks with one `feed` call
/// per chunk.
///
/// Use this helper for high-volume streams such as live logs. It is safe to call
/// ``append(data:)``, ``append(byteArray:)``, or ``append(text:)`` from any queue; the
/// accumulated bytes are flushed to the terminal on the main run loop at a fixed cadence.
/// The timer is installed in `.default` mode so it does not fight scroll tracking on iOS.
public final class TerminalFeedCoalescer {
    private weak var terminalView: TerminalView?
    private let lock = NSLock()
    private var pendingData = Data()
    private var timer: Timer?
    private let runLoopMode: RunLoop.Mode
    private var droppedBytesStorage: UInt64 = 0

    /// How often pending bytes should be flushed to the terminal view.
    public let flushInterval: TimeInterval

    /// Maximum number of pending bytes retained before applying backpressure.
    public let maxPendingBytes: Int

    /// Maximum number of bytes fed to the terminal in a single timer tick.
    ///
    /// This prevents one large backlog from monopolizing the main thread. The
    /// default 64 KiB chunk size keeps parsing work bounded while still providing
    /// high throughput for live log streams.
    public let maxBytesPerFlush: Int

    /// Backpressure strategy used when ``maxPendingBytes`` is exceeded.
    public let backpressurePolicy: TerminalFeedBackpressurePolicy

    /// Number of bytes currently waiting to be flushed.
    public var pendingByteCount: Int {
        lock.lock()
        let count = pendingData.count
        lock.unlock()
        return count
    }

    /// Total number of bytes discarded by backpressure.
    public var droppedByteCount: UInt64 {
        lock.lock()
        let count = droppedBytesStorage
        lock.unlock()
        return count
    }

    /// Creates a coalescer for the given terminal view.
    ///
    /// - Parameters:
    ///   - terminalView: The terminal view to feed. The coalescer keeps this weakly.
    ///   - flushInterval: Flush cadence. The default, 50 ms, caps feed work to about 20 Hz.
    ///   - maxPendingBytes: Backpressure cap for pending bytes. The default is 512 KiB.
    ///   - maxBytesPerFlush: Maximum bytes to feed in one timer tick. The default is 64 KiB.
    ///   - backpressurePolicy: Overflow policy. The default keeps the newest output.
    ///   - runLoopMode: Run-loop mode used by the flush timer. The default is `.default`.
    public init(
        terminalView: TerminalView,
        flushInterval: TimeInterval = 0.05,
        maxPendingBytes: Int = 512 * 1024,
        maxBytesPerFlush: Int = 64 * 1024,
        backpressurePolicy: TerminalFeedBackpressurePolicy = .keepNewest,
        runLoopMode: RunLoop.Mode = .default
    ) {
        self.terminalView = terminalView
        self.flushInterval = max(0.001, flushInterval)
        self.maxPendingBytes = max(1, maxPendingBytes)
        self.maxBytesPerFlush = max(1, maxBytesPerFlush)
        self.backpressurePolicy = backpressurePolicy
        self.runLoopMode = runLoopMode

        DispatchQueue.main.async { [weak self] in
            self?.startTimer()
        }
    }

    deinit {
        timer?.invalidate()
    }

    /// Appends raw bytes from a data buffer.
    public func append(data: Data) {
        append(contentsOf: data)
    }

    /// Appends raw bytes from an array slice.
    public func append(byteArray: ArraySlice<UInt8>) {
        append(contentsOf: byteArray)
    }

    /// Appends UTF-8 encoded text.
    public func append(text: String) {
        append(contentsOf: text.utf8)
    }

    /// Immediately flushes pending bytes to the terminal view on the main queue.
    ///
    /// At most ``maxBytesPerFlush`` bytes are fed per call. If more data remains,
    /// subsequent timer ticks continue draining the buffer without blocking one
    /// main-thread turn for the entire backlog.
    public func flush() {
        if !Thread.isMainThread {
            DispatchQueue.main.async { [weak self] in
                self?.flush()
            }
            return
        }

        let bytes = nextFlushChunk()
        guard !bytes.isEmpty else {
            return
        }

        terminalView?.feed(byteArray: bytes[...])
    }

    /// Flushes all pending bytes in bounded chunks.
    ///
    /// This is useful when a stream ends and the caller wants to drain the buffer.
    /// It still yields one main-queue turn before each chunk when called off the
    /// main thread, and each chunk is capped by ``maxBytesPerFlush``.
    public func flushAll() {
        if !Thread.isMainThread {
            DispatchQueue.main.async { [weak self] in
                self?.flushAll()
            }
            return
        }

        while true {
            let bytes = nextFlushChunk()
            guard !bytes.isEmpty else {
                return
            }
            terminalView?.feed(byteArray: bytes[...])
        }
    }

    /// Stops the timer and discards any pending bytes.
    public func invalidate() {
        if Thread.isMainThread {
            timer?.invalidate()
            timer = nil
        } else {
            DispatchQueue.main.async { [weak self] in
                self?.timer?.invalidate()
                self?.timer = nil
            }
        }

        lock.lock()
        pendingData.removeAll(keepingCapacity: false)
        lock.unlock()
    }

    private func append<S: Sequence>(contentsOf bytes: S) where S.Element == UInt8 {
        lock.lock()
        switch backpressurePolicy {
        case .keepNewest, .unbounded:
            pendingData.append(contentsOf: bytes)
            applyBackpressureIfNeeded()
        case .keepOldest:
            let freeCapacity = maxPendingBytes - pendingData.count
            if freeCapacity <= 0 {
                droppedBytesStorage += UInt64(bytes.underestimatedCount)
            } else {
                var appended = 0
                for byte in bytes {
                    if appended >= freeCapacity {
                        droppedBytesStorage += 1
                        continue
                    }
                    pendingData.append(byte)
                    appended += 1
                }
            }
        }
        lock.unlock()
    }

    private func nextFlushChunk() -> [UInt8] {
        lock.lock()
        defer { lock.unlock() }

        guard !pendingData.isEmpty else {
            return []
        }

        let byteCount = min(maxBytesPerFlush, pendingData.count)
        let chunk = Array(pendingData.prefix(byteCount))
        pendingData.removeFirst(byteCount)
        return chunk
    }

    private func applyBackpressureIfNeeded() {
        guard backpressurePolicy != .unbounded,
              pendingData.count > maxPendingBytes
        else {
            return
        }

        let overflow = pendingData.count - maxPendingBytes
        switch backpressurePolicy {
        case .keepNewest:
            pendingData.removeFirst(overflow)
            droppedBytesStorage += UInt64(overflow)
        case .keepOldest:
            pendingData.removeLast(overflow)
            droppedBytesStorage += UInt64(overflow)
        case .unbounded:
            break
        }
    }

    private func startTimer() {
        guard timer == nil else {
            return
        }

        let newTimer = Timer(timeInterval: flushInterval, repeats: true) { [weak self] _ in
            self?.flush()
        }
        RunLoop.main.add(newTimer, forMode: runLoopMode)
        timer = newTimer
    }
}

public extension TerminalView {
    /// Creates a ``TerminalFeedCoalescer`` for this terminal view.
    ///
    /// Keep a strong reference to the returned object for as long as the stream is active.
    func makeFeedCoalescer(
        flushInterval: TimeInterval = 0.05,
        maxPendingBytes: Int = 512 * 1024,
        maxBytesPerFlush: Int = 64 * 1024,
        backpressurePolicy: TerminalFeedBackpressurePolicy = .keepNewest,
        runLoopMode: RunLoop.Mode = .default
    ) -> TerminalFeedCoalescer {
        TerminalFeedCoalescer(
            terminalView: self,
            flushInterval: flushInterval,
            maxPendingBytes: maxPendingBytes,
            maxBytesPerFlush: maxBytesPerFlush,
            backpressurePolicy: backpressurePolicy,
            runLoopMode: runLoopMode
        )
    }
}
#endif
