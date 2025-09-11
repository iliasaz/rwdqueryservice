import AppAPI
import OpenAPIRuntime
import Foundation
import Logging

// MARK: Implementation of API handlers
struct APIImplementation: APIProtocol {
    let queryEngine: QueryEngine
    let openaiKey: String
    let logger: Logger
    
    /// /values/{attr}
    func listAttributeValues(_ input: AppAPI.Operations.ListAttributeValues.Input) async throws -> AppAPI.Operations.ListAttributeValues.Output {
        
        guard let attrId = queryEngine.dict.attrToID[input.path.attr.rawValue] else {
            return .ok(.init(body: .json([])))
        }
        
        guard let values = queryEngine.dict.valueToID[attrId]?.keys.map({$0}) else {
            return .ok(.init(body: .json([])))
        }
        
        return .ok(.init(body: .json(values)))
    }
    
    /// /eventTypes
    func listEventTypes(_ input: AppAPI.Operations.ListEventTypes.Input) async throws -> AppAPI.Operations.ListEventTypes.Output {
        let eventTypes = AppAPI.Operations.ListEventTypes.Output.Ok.Body.JsonPayloadPayload.allCases
        return .ok(.init(body: .json(eventTypes)))
    }
    
    /// /attributes
    func listAttributes(_ input: AppAPI.Operations.ListAttributes.Input) async throws -> AppAPI.Operations.ListAttributes.Output {
        let attributes = AppAPI.Operations.ListAttributes.Output.Ok.Body.JsonPayloadPayload.allCases
        return .ok(.init(body: .json(attributes)))
    }
    
    /// /query
    func queryPatients(_ input: AppAPI.Operations.QueryPatients.Input) async throws -> AppAPI.Operations.QueryPatients.Output {
        let countOnly = input.query.countOnly ?? true
        
        let inputBody = input.body
        switch inputBody {
            case .json(let queryRequest):
                let queryResults = queryEngine.queryFromPayload(queryRequest: queryRequest, countOnly: countOnly)
                return .ok(.init(body: .json(queryResults)))
        }
    }
    
    /// /ask
    func ask(_ input: AppAPI.Operations.Ask.Input) async throws -> AppAPI.Operations.Ask.Output {
        let conversationId = "mock-conversation-001"
        let body = input.body
        switch body {
            case .json(let payload):
                let messages = payload.context
                let agent = Agent(apiKey: openaiKey, queryEngine: queryEngine, logger: logger)
                let agentResponse = try await agent.ask(context: messages)
                return .ok(.init(body: .json(AppAPI.Operations.Ask.Output.Ok.Body.JsonPayload(message: agentResponse))))
        }
    }
    
    /// /health
    func getHealth(_ input: AppAPI.Operations.GetHealth.Input) async throws -> AppAPI.Operations.GetHealth.Output {
        return .ok(.init(body: .plainText("ok")))
    }
}
