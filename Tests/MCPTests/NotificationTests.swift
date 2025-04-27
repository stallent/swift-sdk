import Testing

import class Foundation.JSONDecoder
import class Foundation.JSONEncoder

@testable import MCP

@Suite("Notification Tests")
struct NotificationTests {
    struct TestNotification: Notification {
        struct Parameters: Codable, Hashable, Sendable {
            let event: String
        }
        static let name = "test.notification"
    }

    struct InitializedNotification: Notification {
        static let name = "notifications/initialized"
    }

    @Test("Notification initialization with parameters")
    func testNotificationWithParameters() throws {
        let params = TestNotification.Parameters(event: "test-event")
        let notification = TestNotification.message(params)

        #expect(notification.method == TestNotification.name)
        #expect(notification.params.event == "test-event")

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let data = try encoder.encode(notification)
        let decoded = try decoder.decode(Message<TestNotification>.self, from: data)

        #expect(decoded.method == notification.method)
        #expect(decoded.params.event == notification.params.event)
    }

    @Test("Empty parameters notification")
    func testEmptyParametersNotification() throws {
        struct EmptyNotification: Notification {
            static let name = "empty.notification"
        }

        let notification = EmptyNotification.message()

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let data = try encoder.encode(notification)
        let decoded = try decoder.decode(Message<EmptyNotification>.self, from: data)

        #expect(decoded.method == notification.method)
    }

    @Test("Initialized notification encoding")
    func testInitializedNotificationEncoding() throws {
        let notification = InitializedNotification.message()

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let data = try encoder.encode(notification)

        // Verify the exact JSON structure
        let json = try JSONDecoder().decode([String: Value].self, from: data)
        #expect(json["jsonrpc"] == "2.0")
        #expect(json["method"] == "notifications/initialized")
        #expect(json.count == 2, "Should only contain jsonrpc and method fields")

        // Verify we can decode it back
        let decoded = try decoder.decode(Message<InitializedNotification>.self, from: data)
        #expect(decoded.method == InitializedNotification.name)
    }

    @Test("Initialized notification decoding")
    func testInitializedNotificationDecoding() throws {
        // Create a minimal JSON string
        let jsonString = """
            {"jsonrpc":"2.0","method":"notifications/initialized"}
            """
        let data = jsonString.data(using: .utf8)!

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(Message<InitializedNotification>.self, from: data)

        #expect(decoded.method == InitializedNotification.name)
    }

    @Test("Resource updated notification with parameters")
    func testResourceUpdatedNotification() throws {
        let params = ResourceUpdatedNotification.Parameters(uri: "test://resource")
        let notification = ResourceUpdatedNotification.message(params)

        #expect(notification.method == ResourceUpdatedNotification.name)
        #expect(notification.params.uri == "test://resource")

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()

        let data = try encoder.encode(notification)

        // Verify the exact JSON structure
        let json = try JSONDecoder().decode([String: Value].self, from: data)
        #expect(json["jsonrpc"] == "2.0")
        #expect(json["method"] == "notifications/resources/updated")
        #expect(json["params"] != nil)
        #expect(json.count == 3, "Should contain jsonrpc, method, and params fields")

        // Verify we can decode it back
        let decoded = try decoder.decode(Message<ResourceUpdatedNotification>.self, from: data)
        #expect(decoded.method == ResourceUpdatedNotification.name)
        #expect(decoded.params.uri == "test://resource")
    }
}
