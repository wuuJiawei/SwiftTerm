//
//  TerminalFeedCoalescer.swift
//  SwiftTerm
//
//  Coalesces high-frequency terminal output before feeding TerminalView.
//

#if os(macOS) || os(iOS) || os(visionOS)
import Foundation

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
    private var pendingBytes: [UInt8] = []
    private var timer: Timer?
    private let runLoopMode: RunLoop.Mode

    /// How often pending bytes should be flushed to the terminal view.
    public let flushInterval: TimeInterval

    /// Maximum number of pending bytes to retain before dropping the oldest bytes.
    ///
    /// This is a backpressure guard for workloads where the producer is faster than the UI.
    /// It preserves the newest output, which is normally what live log viewers need most.
    public let maxPendingBytes: Int

    /// Creates a coalescer for the given terminal view.
    ///
    /// - Parameters:
    ///   - terminalView: The terminal view to feed. The coalescer keeps this weakly.
    ///   - flushInterval: Flush cadence. The default, 50 ms, caps feed work to about 20 Hz.
    ///   - maxPendingBytes: Backpressure cap for pending bytes. The default is 512 KiB.
    ///   - runLoopMode: Run-loop mode used by the flush timer. The default is `.default`.
    public init(
        terminalView: TerminalView,
        flushInterval: TimeInterval = 0.05,
        maxPendingBytes: Int = 512 * 1024,
        runLoopMode: RunLoop.Mode = .default
    ) {
        self.terminalView = terminalView
        self.flushInterval = max(0.001, flushInterval)
        self.maxPendingBytes = max(1, maxPendingBytes)
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

    /// Immediately flushes all pending bytes to the terminal view on the main queue.
    public func flush() {
        if !Thread.isMainThread {
            DispatchQueue.main.async { [weak self] in
                self?.flush()
            }
            return
        }

        let bytes: [UInt8]
        lock.lock()
        if pendingBytes.isEmpty {
            lock.unlock()
            return
        }
        bytes = pendingBytes
        pendingBytes.removeAll(keepingCapacity: true)
        lock.unlock()

        terminalView?.feed(byteArray: bytes[...])
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
        pendingBytes.removeAll(keepingCapacity: false)
        lock.unlock()
    }

    private func append<S: Sequence>(contentsOf bytes: S) where S.Element == UInt8 {
        lock.lock()
        pendingBytes.append(contentsOf: bytes)
        trimPendingBytesIfNeeded()
        lock.unlock()
    }

    private func trimPendingBytesIfNeeded() {
        guard pendingBytes.count > maxPendingBytes else {
            return
        }
        pendingBytes.removeFirst(pendingBytes.count - maxPendingBytes)
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
        runLoopMode: RunLoop.Mode = .default
    ) -> TerminalFeedCoalescer {
        TerminalFeedCoalescer(
            terminalView: self,
            flushInterval: flushInterval,
            maxPendingBytes: maxPendingBytes,
            runLoopMode: runLoopMode
        )
    }
}
#endif
