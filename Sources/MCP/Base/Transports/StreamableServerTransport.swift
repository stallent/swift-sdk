//
//  StreamableTransport.swift
//  hummingbird_mcp
//
//  Created by Stephen Tallent on 5/8/25.
//

import Foundation
import Logging

enum TransportError : Swift.Error {
    case notificationStreamAlreadyConnected
    case noResponseStreamAvailable
    case noRequestOrMessageFound
}

// Type sugar
public typealias StreamID = UUID
public typealias RequestID = ID
public typealias DataStream = AsyncThrowingStream<Data, any Swift.Error>
public typealias DataStreamPackage = (stream: DataStream,  continuation: DataStream.Continuation)
public typealias StreamInfo = (streamID: StreamID, stream: DataStream)

public actor StreamableServerTransport : ServerTransport {
    
    public var logger: Logging.Logger
    
    // the stream consumed by the Server class for sending messages into it
    private let serverStreamPackage: DataStreamPackage = DataStream.makeStream()
    
    // the GET stream used for sending notifications to the client if they open one up.
    private var noteStreamPackage: DataStreamPackage?

    // POSTS create a "short lived" stream for responding to requests. Each post creates a UUID identifier
    // and an AsyncStream. The streamMap holds onto those. A POST can include multiple requests batched into
    // it. The requestToStreamMap keeps up with each request's id mapped to the stream it needs to write to
    private var streamMap: [StreamID:DataStreamPackage] = [:]
    private var requestToStreamMap: [RequestID:StreamID] = [:]
    
    public init() {
        self.logger = Logging.Logger(label: "StreamableTransport")
       
    }
    
    public func connect() async throws {}
    
    public func disconnect() async {}
    
    public func send(_ data: Data) async throws {
        self.noteStreamPackage?.continuation.yield(data)
    }
    
    public func send(requestId: RequestID, data: Data) async throws {
        // grab the id of the stream we want to write to logged when the request came in (see handlePost below)
        guard let streamID = self.requestToStreamMap[requestId] else { throw TransportError.noResponseStreamAvailable }
        // and grab the stream itself
        guard let stream = self.streamMap[streamID] else { throw TransportError.noResponseStreamAvailable }
        
        // write the data (could be a response or a notification/message)
        stream.continuation.yield(data)
        
        // If its a response, we have some housekeeping to do
        if let _ = try? JSONDecoder().decode(AnyResponse.self, from: data) {

            // go ahead and clear out the mapping of this specific request to its stream
            self.requestToStreamMap[requestId] = nil

            // send a POST can contain a batch of requests, we need to see if there
            // are any others requests who have not had their response written to yet.
            let relatedRequests = self.requestToStreamMap.filter { $1 == streamID }
            
            // if none are left, then either there was only one request in the POST, or
            // all the ones from the batch have now been responsed do so we can finish
            // the stream and remove it
            if relatedRequests.count == 0 {
                stream.continuation.finish()
                self.streamMap[streamID] = nil
            }
        }
       
    }
    
    public func receive() -> AsyncThrowingStream<Data, any Swift.Error> {
        return serverStreamPackage.stream
    }
    
    //------------------------
    
    public func handlePost(data:Data) async throws -> StreamInfo? {
        
        // So we need to set up some request to response stream mapping
        // here so we can write the responses to the correct place later.
        
        // since batches CAN happen, its simpler just to go ahead and create
        // an array we can handle after the decoding part below
        var items:[Server.Batch.Item] = []
        
        // I don't like having to double decode this stuff (here, and then later
        // again within the server). the official TS client does all of this
        // in the transport layer and much less in the core server code. If we
        // want to avoid this, we may end up having to have a different Transport
        // protocol for servers instead of trying to have a common one between server and client.
        let decoder = JSONDecoder()
        if let batch = try? decoder.decode(Server.Batch.self, from: data) {
            items.append(contentsOf: batch.items)
        } else if let request = try? decoder.decode(AnyRequest.self, from: data) {
            items.append(.request(request))
        } else if let message = try? decoder.decode(AnyMessage.self, from: data) {
            items.append(.notification(message))
        } else {
            throw TransportError.noRequestOrMessageFound
        }
        
        // generate a new stream and ID specifically for this request
        let streamID:StreamID = UUID()
        let responseStream:DataStreamPackage = DataStream.makeStream()
        
        // cache it
        self.streamMap[streamID] = responseStream
        
        
        var hasRequests:Bool = false
        for item in items {
            switch item {
                case .request(let request):
                    self.requestToStreamMap[request.id] = streamID
                    hasRequests = true
                
                case .notification(let message):
                    break // not really doing anything here
            }
        }
        
        
        // send things on in
        serverStreamPackage.continuation.yield(data)
        
        // return a stream if we are going to have responses.
        // otherwise return nil
        return hasRequests ? (streamID, responseStream.stream) : nil
    }
    
    public func handleGet() async throws -> DataStream {
        guard noteStreamPackage == nil else { throw MCPError.transportError(TransportError.notificationStreamAlreadyConnected) }
        
        let stream:DataStreamPackage = DataStream.makeStream()
        noteStreamPackage = stream
        
        return stream.stream
    }
    
    public func endGet() {
        noteStreamPackage?.continuation.finish()
        noteStreamPackage = nil
    }
    
    
    public func isGetConnected() -> Bool {
        return noteStreamPackage != nil
    }
 
 }
