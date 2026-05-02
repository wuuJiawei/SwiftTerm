//
//  CoalescedLocalProcessTerminalView.swift
//  SwiftTerm
//
//  Low-latency output batching for LocalProcessTerminalView.
//

#if os(macOS)
import Foundation
import AppKit

/// A ``LocalProcessTerminalView`` variant that batches host output before feeding
/// it into SwiftTerm's parser and renderer.
///
/// This is useful for interactive shells that sometimes feel like typed
/// characters are not echoing back smoothly. The class keeps output ordering,
/// flushes frequently, and caps each main-thread feed chunk so bursts do not
/// monopolize the UI thread.
open class CoalescedLocalProcessTerminalView: LocalProcessTerminalView {
    private var outputCoalescer: TerminalFeedCoalescer?

    /// Enables or disables coalescing. Defaults to `true`.
    public var coalescesOutput: Bool = true

    /// Creates the output coalescer lazily after the superclass setup has
    /// created the terminal view internals.
    private func ensureOutputCoalescer() -> TerminalFeedCoalescer {
        if let outputCoalescer {
            return outputCoalescer
        }

        let coalescer = makeFeedCoalescer(
            flushInterval: 1.0 / 120.0,
            maxPendingBytes: 256 * 1024,
            maxBytesPerFlush: 16 * 1024,
            backpressurePolicy: .keepOldest
        )
        outputCoalescer = coalescer
        return coalescer
    }

    /// Receives output from the host side and feeds it through a low-latency,
    /// bounded coalescer instead of calling `feed(byteArray:)` directly.
    open override func dataReceived(slice: ArraySlice<UInt8>) {
        guard coalescesOutput else {
            super.dataReceived(slice: slice)
            return
        }

        ensureOutputCoalescer().append(byteArray: slice)
    }

    open override func processTerminated(_ source: LocalProcess, exitCode: Int32?) {
        outputCoalescer?.flushAll()
        super.processTerminated(source, exitCode: exitCode)
    }

    public override func terminate() {
        outputCoalescer?.flushAll()
        super.terminate()
    }
}

#endif
