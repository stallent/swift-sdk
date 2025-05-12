import Logging

import struct Foundation.Data

/// Protocol defining the transport layer for MCP communication
public protocol Transport: Actor {
    var logger: Logger { get }

    /// Establishes connection with the transport
    func connect() async throws

    /// Disconnects from the transport
    func disconnect() async

    /// Sends data
    func send(_ data: Data) async throws

    /// Receives data in an async sequence
    func receive() -> AsyncThrowingStream<Data, Swift.Error>
}

// Temporary Solution until discussion is had.
public protocol ServerTransport : Transport {
    
    // this allows the server to give details to the transport
    // so it can know which stream to write to.
    func send(requestId:RequestID, data: Data) async throws
    
}
