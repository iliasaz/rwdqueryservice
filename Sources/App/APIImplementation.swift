import AppAPI
import OpenAPIRuntime
import Foundation

struct APIImplementation: APIProtocol {
    let queryEngine: QueryEngine
    
    func listAttributeValues(_ input: AppAPI.Operations.ListAttributeValues.Input) async throws -> AppAPI.Operations.ListAttributeValues.Output {
        
        guard let attrId = queryEngine.dict.attrToID[input.path.attr.rawValue] else {
            return .ok(.init(body: .json([])))
        }
        
        guard let values = queryEngine.dict.valueToID[attrId]?.keys.map({$0}) else {
            return .ok(.init(body: .json([])))
        }

        return .ok(.init(body: .json(values)))
    }
    
    func listEventTypes(_ input: AppAPI.Operations.ListEventTypes.Input) async throws -> AppAPI.Operations.ListEventTypes.Output {
        let eventTypes = AppAPI.Operations.ListEventTypes.Output.Ok.Body.JsonPayloadPayload.allCases
        return .ok(.init(body: .json(eventTypes)))
    }
    
    func listAttributes(_ input: AppAPI.Operations.ListAttributes.Input) async throws -> AppAPI.Operations.ListAttributes.Output {
        let attributes = AppAPI.Operations.ListAttributes.Output.Ok.Body.JsonPayloadPayload.allCases
        return .ok(.init(body: .json(attributes)))
    }
    
    func queryPatients(_ input: AppAPI.Operations.QueryPatients.Input) async throws -> AppAPI.Operations.QueryPatients.Output {
        let countOnly = input.query.countOnly ?? true
        
        let inputBody = input.body
        switch inputBody {
        case .json(let payload):
            let queryResponse = queryEngine.queryFromPayload(payload: payload, countOnly: countOnly)
            return .ok(.init(body: .json(queryResponse)))
        }
    }
    
    func getHealth(_ input: AppAPI.Operations.GetHealth.Input) async throws -> AppAPI.Operations.GetHealth.Output {
        return .ok(.init(body: .plainText("ok")))
    }
    
    func getHello(_ input: AppAPI.Operations.GetHello.Input) async throws -> AppAPI.Operations.GetHello.Output {
        return .ok(.init(body: .plainText("Hello!")))
    }
    
    
}
