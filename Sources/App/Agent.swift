//
//  Agent.swift
//  rwdqueryservice
//
//  Created by Ilia Sazonov on 9/8/25.
//

import Foundation
import AppAPI
import OpenAI
import Logging

typealias AgentMessage = AppAPI.Components.Schemas.Message
typealias QueryRequest = AppAPI.Components.Schemas.QueryRequest
typealias QueryResults = AppAPI.Components.Schemas.QueryResults

/// An LLM-backed agent that interacts with the RWD query service.
/// It follows a lightweight agentic plan inspired by OpenAI's GPT‑5 prompting guidelines,
/// normalizes medical codes, maps common clinical terms to codes, and calls local tools
/// The main method `ask(context:)` consumes conversation history, decides whether to
/// clarify or run a query, composes a `QueryRequest`, executes it, and returns
/// a `Components.Schemas.Message` with `proposedQuery` and (optionally) `queryResults`.
struct Agent: @unchecked Sendable {
    let openai: OpenAI
    let queryEngine: QueryEngine
    let logger: Logger
    
    init(apiKey: String, queryEngine: QueryEngine, logger: Logger) {
//        self.openai = OpenAI(apiToken: apiKey)
        self.openai = OpenAI(configuration: .init(token: apiKey, timeoutInterval: 300), middlewares: [LoggingMiddleware()])
        self.queryEngine = queryEngine
        self.logger = logger
        
        // get allowable attribute values from the query engine
        var tempAttrVals = [String: [String]]()
        for attrName in AppAPI.Operations.ListAttributeValues.Input.Path.AttrPayload.allCases {
            if attrName == .state {
                tempAttrVals[attrName.rawValue, default: []] = ["<list of abbreviated US states, eg. AZ, CA, and etc.>"]
            } else if attrName == .yearOfBirth {
                tempAttrVals[attrName.rawValue, default: []] = ["<4-digit year>"]
            } else {
                let attrId = queryEngine.dict.attrToID[attrName.rawValue]!
                tempAttrVals[attrName.rawValue, default: []] = queryEngine.dict.valueToID[attrId]?.keys.map({$0.description}) ?? []
            }
        }
        attributeValues = tempAttrVals
        
        queryRequestSchema =  try! derivedQueryRequestSchema()
        queryTool = FunctionTool(name: "QueryPatients", parameters: queryRequestSchema, strict: false)
        
        let derivedSchemaStr = (try? derivedQueryRequestSchemaJSONString(prettyPrinted: true)) ?? ""
        logger.debug("derivedSchema:\n\(derivedSchemaStr)\n\n")
    }
    
    private let queryRequestSchema: JSONSchema
    private let queryTool: FunctionTool
    private let attributeValues: [String: [String]]
    
    private var attributeValueList: String {
        var list = ""
        for (key, values) in attributeValues {
            list += "\(key): [\(values.joined(separator: ", "))]\n"
        }
        return list
    }
    
    // MARK: - High-level agent method
    
    /// Process a conversation context and produce an agent response message.
    /// This method:
    /// 1) Reads the last user message.
    /// 2) Uses prompt templates to decide whether to query or clarify via an LLM.
    /// 3) If querying, builds a QueryRequest, normalizes codes, calls /query,
    ///    and returns a message with proposedQuery and queryResults.
    /// 4) On errors, returns a clarification message with suggestions.
    ///
    /// - Parameter context: The conversation context as an array of Message objects.
    /// - Returns: An agent Message response.
    func ask(context: [AgentMessage]) async throws -> AgentMessage {
        guard !context.isEmpty else {
            return AgentMessage(
                role: .agent,
                content: "I didn’t receive a user message to act on. Please provide your question."
            )
        }
        
        let instructions = [
            SYSTEM_PROMPT,
        ].joined(separator: "\n\n")
        
        do {
            let request = CreateModelResponseQuery(
                input: makeModelInput(from: context),
                model: .gpt5,
                instructions: instructions.replacingOccurrences(of: "[ATTRIBUTE_VALUE_LIST]", with: self.attributeValueList),
                reasoning: .init(effort: .low),
                text: .text,
                toolChoice: .ToolChoiceOptions(.auto),
                tools: [.functionTool(.init(name: "QueryPatients", parameters: queryRequestSchema, strict: true))]
            )
            
            logger.debug("Submitted request to LLM")
//            logger.debug("\n\n=============\n\(request)\n===============\n\n")
            let result: ResponseObject = try await openai.responses.createResponse(query: request)
            logger.debug("Outputs: \(result.output.count)")
            
            var agentResponse = AgentMessage(role: .agent, content: "")
            for output in result.output  {
                switch output {
                    case .outputMessage(let outputMessage):
                        for content in outputMessage.content {
                            switch content {
                                case .OutputTextContent(let textContent):
                                    agentResponse.content?.append(textContent.text)
                                case .RefusalContent(let refusalContent):
                                    logger.info("LLM refused to reply: \(refusalContent.refusal)")
                            }
                        }
                    case .functionToolCall(let functionCall):
                        do {
                            guard let argData = functionCall.arguments.data(using: .utf8) else {
                                logger.error("Cannot decode UTF8 from functionCall.arguments")
                                break
                            }
                            let queryRequest = try JSONDecoder().decode(QueryRequest.self, from: argData)
                            let queryResults = queryEngine.queryFromPayload(queryRequest: queryRequest, countOnly: true)
                            agentResponse.proposedQuery = queryRequest
                            agentResponse.queryResults = queryResults
                            logger.debug("query results: \(queryResults)")
                        } catch {
                            logger.error("Cannot decode QueryRequest from functionCall.arguments or QueryEngine request failed")
                            // TODO: retry LLM call
                        }
                    case .reasoning(let reasoning):
                        // ignoring for now
                        print("reasoning: \(reasoning.summary)")
                    default:
                        // Unhandled output items. Handle or throw an error.
                        logger.error("Unhandled output: \(output)")
                }
            }
            
            if !(agentResponse.content ?? "").isEmpty && (agentResponse.proposedQuery == nil || agentResponse.queryResults == nil) {
                // something went wrong
                logger.debug("The agent didn't produce the query output")
                return agentResponse
            }
            
            if agentResponse.proposedQuery != nil && agentResponse.queryResults != nil && (agentResponse.content ?? "").isEmpty {
                // Get concise guidance on results and next steps using the follow-up prompt
                let request = CreateModelResponseQuery(
                    input: makeFollowupModelInput(from: context, with: agentResponse),
                    model: .gpt5,
                    instructions: RESULT_FOLLOWUP,
                    reasoning: .init(effort: .none),
                    text: .text
                )
                let result: ResponseObject = try await openai.responses.createResponse(query: request)
                for output in result.output {
                    switch output {
                        case .outputMessage(let outputMessage):
                            for content in outputMessage.content {
                                switch content {
                                    case .OutputTextContent(let textContent):
                                        agentResponse.content?.append(textContent.text)
                                    case .RefusalContent(let refusalContent):
                                        logger.info("LLM refused to reply: \(refusalContent.refusal)")
                                }
                            }
                        default: continue
                    }
                }
            }
            return agentResponse
        } catch {
            return Components.Schemas.Message(
                role: .agent,
                content: "Unexpected error contacting the LLM: \(error.localizedDescription)"
            )
        }
    }
        
    // MARK: - Helper methods
    
    /// Normalize a medical code string by replacing all 'x' or 'X' characters with '*'.
    /// For example, "E11x" -> "E11*"
    /// - Parameter code: The input medical code string.
    /// - Returns: The normalized code string.
    func normalizeCode(_ code: String) -> String {
        return code.map { char in
            if char == "x" || char == "X" {
                return "*"
            } else {
                return String(char)
            }
        }.joined()
    }
    
    private func extractFirstJSONObject(from text: String) -> String? {
        guard let startIdx = text.firstIndex(of: "{") else { return nil }
        var depth = 0
        var endIdx: String.Index?
        var idx = startIdx
        while idx < text.endIndex {
            let ch = text[idx]
            if ch == "{" { depth += 1 }
            if ch == "}" {
                depth -= 1
                if depth == 0 {
                    endIdx = idx
                    break
                }
            }
            idx = text.index(after: idx)
        }
        if let endIdx {
            return String(text[startIdx...endIdx])
        }
        return nil
    }
        
    private func normalizeProposedQuery(_ req: AppAPI.Components.Schemas.QueryRequest) -> AppAPI.Components.Schemas.QueryRequest {
        func normalizeEventFilters(_ filters: AppAPI.Components.Schemas.EventFilters?) -> AppAPI.Components.Schemas.EventFilters? {
            guard let filters else { return nil }
            func norm(_ list: [AppAPI.Components.Schemas.EventFilter]?) -> [AppAPI.Components.Schemas.EventFilter]? {
                guard let list else { return nil }
                return list.map { ef in
                    AppAPI.Components.Schemas.EventFilter(
                        attr: ef.attr,
                        value: normalizeCode(ef.value),
                        startYYYYMM: ef.startYYYYMM,
                        endYYYYMM: ef.endYYYYMM
                    )
                }
            }
            return Components.Schemas.EventFilters(
                allOf: norm(filters.allOf),
                anyOf: norm(filters.anyOf),
                exclude: norm(filters.exclude)
            )
        }
        
        return Components.Schemas.QueryRequest(
            attributes: req.attributes,
            events: normalizeEventFilters(req.events)
        )
    }

    private func makeModelInput(from context: [AgentMessage]) -> CreateModelResponseQuery.Input {
        var items: [InputItem] = []
        items.reserveCapacity(context.count)
        
        for msg in context {
            guard let role = mapRole(msg.role) else { continue }
            let text = renderMessageContentForLLM(msg)
            let inputMsg = EasyInputMessage(role: role, content: .textInput(text))
            items.append(.inputMessage(inputMsg))
        }
        
        return .inputItemList(items)
    }
    
    private func makeFollowupModelInput(from context: [AgentMessage], with response: AgentMessage) -> CreateModelResponseQuery.Input {
        var items: [InputItem] = []
        items.reserveCapacity(context.count + 1)
        
        // Include full prior conversation
        for msg in context {
            guard let role = mapRole(msg.role) else { continue }
            let text = renderMessageContentForLLM(msg)
            let inputMsg = EasyInputMessage(role: role, content: .textInput(text))
            items.append(.inputMessage(inputMsg))
        }
        
        // Append the assistant's structured output (proposedQuery + queryResults) we just computed
        let appendedAssistantText = renderMessageContentForLLM(response)
        let assistantMsg = EasyInputMessage(role: .assistant, content: .textInput(appendedAssistantText))
        items.append(.inputMessage(assistantMsg))
        
        return .inputItemList(items)
    }
    
    private func mapRole(_ role: AppAPI.Components.Schemas.Message.RolePayload?) -> EasyInputMessage.RolePayload? {
        switch role {
        case .user: return .user
        case .agent: return .assistant
        case .system: return .system
        default: return nil
        }
    }
    
    private func renderMessageContentForLLM(_ msg: AgentMessage) -> String {
        var parts: [String] = []
        if let content = msg.content, !content.isEmpty {
            parts.append(content)
        }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        if let pq = msg.proposedQuery, let data = try? encoder.encode(pq), let s = String(data: data, encoding: .utf8) {
            parts.append("proposedQuery:\n\(s)")
        }
        if let qr = msg.queryResults, let data = try? encoder.encode(qr), let s = String(data: data, encoding: .utf8) {
            parts.append("queryResults:\n\(s)")
        }
        return parts.joined(separator: "\n\n")
    }
    
    // MARK: - Static prompt templates
    
    /// System prompt that encodes role, plan, tool usage, persistence, and concise final answers.
    /// This mirrors patterns from the GPT‑5 Prompting Guide (tool preambles, planning, persistence,
    /// limiting eagerness, and clear stop conditions).
    
    private let SYSTEM_PROMPT = """
    You are an expert in clinical informatics, data analysis and reporting. Your job is help a user to translate their search request into a **structured** set of inclusion/exclusion conditions. If the user asks about anything else, politely and briefly respond that you don't have expertise in other fields and do not call any tools. 
    If the user question is indeed about patient query or cohort building, analyze the request with the objective of forming a well structured query and call QueryPatients tool for executing the query. The criteria may include the following elements:
    
      - Patient demographics attributes such as gender, race, ethnicity, yearOfBirth, state, metro, urban. The user doesn't necessarily know the exact values for these attributes, and you need to map the user request to the allowed values of the attributes. For example, "show me whilte women in rural areas of Arizona and California" should translate to the following structure:
    ```json
    "attributes": {
        "allOf": [
          {
            "attr": "gender",
            "value": "Female"
          },
          {
            "attr": "race",
            "value": "White"
          },
          {
            "attr": "urban",
            "value": "rural"
          }
        ],
        "anyOf": [
          {
            "attr": "state",
            "value": "AZ"
          },
          {
            "attr": "state",
            "value": "CA"
          }
      }
    ```
    Remember to use `anyOf` for lists and OR operations. Here is a list of allowed attribute values. 
    [ATTRIBUTE_VALUE_LIST]
    Attribute `state` has abbreviated US states, eg. AZ, CA, TX, and etc. 
    Attribute yearOfBirth just a 4-digit year of birht, eg. 1948, 2005, and etc.
    
      - Patient Events such as diagnoses, procedures, medications. Pass the event code type in the `attr` property. Allowed values for 'attr' property in `events` object are: conditionCode, medicationCode, procedureCode. Events are codifed with one or more common ontologies like ICD10 for conditions and diagnoses, RxNorm for medications (drugs), CPT for procedures. The user input may have a mixture of codified values like ICD10 codes for diagnosis (eg. E11.*), RxNorm codes for drugs and medications and CPT code for procedures as well as spelled out or abbreviated medical terms. You must substitute the terms with the corresponding codes based on your best judgment. Use the wildcard '*' symbol at the end of the code whenever applicable. Do NOT include the ontology name in the code. 
           
    The events may also include a date range expressed as startYYYYMM and endYYYYMM values in YYYYMM (year and month) format. For example, a user ias asking about patients diagnosed with asthma in the past 2 years.
    
    User: "Patients diagnosed with asthma in the past 2 years"
    Today = 2025-09-11
    "events": {
    "allOf": [
    {
      "attr": "conditionCode",
      "value": "J45.*",
      "startYYYYMM": 202309,
      "endYYYYMM": 202509
    }
    ]
    }
        
    If the user mentions ANY temporal reference (absolute dates like “from 2019 to 2023” OR relative periods like “last 2 years”), you MUST output BOTH `startYYYYMM` and `endYYYYMM`. 
    - Convert relative time to absolute year-month using TODAY = 2025-09-11. 
    - Use YYYYMM format, always 6 digits, with zero padding.
    - If you cannot determine a period, return null explicitly for both.
    
    Instructions
    Step 1: Understand the user question. Verify that the user is asking a question about patient population. If the user asks for something else, politely and briefly respond that you don't have expertise in other fields. 
    Step 2: Decide whether you have enough information to call QueryPatients tool now. If not, ask for at most 2 clarifications (brief) and propose defaults where reasonable.
    Step 3: Extract demographics attributes.
    Step 4: Extract events and map medical terms to codes. Infer common codes when user provides medical terms. Use wildcards if necessary. Eg., "type 2 diabetes" → E11.*. Normalize code wildcard symbol (x/X → *). Do NOT prefix codes with the coding system. For example, for type 2 diabetes, use E11.*, not ICD10:E11.*. For medicationCode use RxNorm codes directly, eg., for ibuprofen use 5640, not RxNorm:5640. For procedures use CPT codes directly. eg., for kidney transplant use 50320, not CPT:50320.
    Step 3: Extract temporal constraints. If any, normalize into {startYYYYMM, endYYYYMM}. Convert relative time to absolute year-month using TODAY = 2025-09-11.
    Step 5: Compose conditions into allOf/anyOf/exclude blocks properly. Always translate IN lists to `anyOf` condition block.
    Step 4: Compose QueryPatients JSON with attributes + events.
    
    Always bias toward the accurate representation of the user request. Make sure to capture ALL relevant information accurately.
    ⚠️ If you omit startYYYYMM or endYYYYMM when a period is present, your output will be rejected. Always resolve relative time into YYYYMM based on TODAY = 2025-09-11.
    """
    
    private let RESULT_FOLLOWUP = """
    You are an expert in clinical informatics expert and data analysis and reporting. You have just completed calculating a patient cohort size based on the user input.
    
    Instructions:
    - Analyze the results. If the result count is 0, it may indicate a problem with the formulation of the request or the query. In this case, critically review the user input and the query formulation. Provide consise recommendation and ask the user if they would like to proceed.
    - If the result count looks reasonable, suggest 1-2 options to further refine the cohort definition but stay within the available attributes and event types. 
    ⚠️ Do NOT suggest other attributes or events that are not available in the dataset. 
    ⚠️ Do NOT suggest correlated or nested criteria. Make the suggestions brief.
    - Available Atributes: gender, race, ethnicity, state, metro, urban.
    - Available Events: conditionCode, medicationCode, procedureCode.
    - Politely ask if the user would like to go on with either option or if they need other help.
    """
}
