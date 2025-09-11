import AppAPI
import OpenAPIRuntime
import Foundation

// MARK: Implementation of API handlers
struct APIImplementation: APIProtocol {
    let queryEngine: QueryEngine
    
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
    
    func ask(_ input: AppAPI.Operations.Ask.Input) async throws -> AppAPI.Operations.Ask.Output {
        let conversationId = "mock-conversation-001"
        
        let proposedQuery = Components.Schemas.QueryRequest(
            attributes: Components.Schemas.AttributeFilters(
                allOf: [
                    Components.Schemas.AttrVal(attr: .gender, value: "Male"),
                    Components.Schemas.AttrVal(attr: .race, value: "Black or African American")
                ]
            ),
            events: Components.Schemas.EventFilters(
                anyOf: [
                    Components.Schemas.EventFilter(attr: .conditionCode, value: "E11", startYyyymm: 202201, endYyyymm: 202412)
                ]
            )
        )
        
        let queryResults = Components.Schemas.QueryResults(
            count: 4,
            patients: ["patientA", "patientB", "patientC", "patientD"]
        )
        
        let message = Components.Schemas.Message(
            role: .agent,
            content: "Here are black male patients with Type 2 diabetes mellitus in 2022.",
            proposedQuery: proposedQuery,
            queryResults: queryResults
        )
        
        let responseBody = AppAPI.Operations.Ask.Output.Ok.Body.JsonPayload(
            conversationId: conversationId,
            message: message
        )
        return .ok(.init(body: .json(responseBody)))
    }
    
    /// /events/search
    func searchEvents(_ input: AppAPI.Operations.SearchEvents.Input) async throws -> AppAPI.Operations.SearchEvents.Output {
        // Extract query params with defaults
        let eventType = input.query.eventType.rawValue
        let keyword = input.query.keyword.lowercased()
        let matchMode = input.query.match?.rawValue ?? "prefix"
        let limit = max(1, min(input.query.limit ?? 20, 100))
        let offset = max(0, input.query.offset ?? 0)
        
        guard !keyword.isEmpty else {
            let response = AppAPI.Operations.SearchEvents.Output.Ok.Body.JsonPayload(values: [], total: 0)
            return .ok(.init(body: .json(response)))
        }
        
        // Lookup attribute ID for the selected event type and its values
        guard let attrId = queryEngine.dict.attrToID[eventType],
              let vmap = queryEngine.dict.valueToID[attrId] else {
            let response = AppAPI.Operations.SearchEvents.Output.Ok.Body.JsonPayload(values: [], total: 0)
            return .ok(.init(body: .json(response)))
        }
        
        // Filter values case-insensitively
        let allValues = Array(vmap.keys)
        let prefixMatches = allValues.filter { $0.lowercased().hasPrefix(keyword) }.sorted()
        
        let matches: [String]
        if matchMode == "contains" {
            let substrMatches = allValues.filter { val in
                let lower = val.lowercased()
                return lower.contains(keyword) && !lower.hasPrefix(keyword)
            }.sorted()
            matches = prefixMatches + substrMatches
        } else {
            matches = prefixMatches
        }
        
        // Paging
        let total = matches.count
        let start = min(offset, total)
        let end = min(start + limit, total)
        let page = Array(matches[start..<end])
        
        let response = AppAPI.Operations.SearchEvents.Output.Ok.Body.JsonPayload(values: page, total: total)
        return .ok(.init(body: .json(response)))
    }
    
    /// /health
    func getHealth(_ input: AppAPI.Operations.GetHealth.Input) async throws -> AppAPI.Operations.GetHealth.Output {
        return .ok(.init(body: .plainText("ok")))
    }
}
