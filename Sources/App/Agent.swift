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
        self.openai = OpenAI(configuration: .init(token: apiKey), middlewares: [LoggingMiddleware()])
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
        guard let userInput = context.last(where: { $0.role == .user })?.content else {
            return AgentMessage(
                role: .agent,
                content: "I didn’t receive a user message to act on. Please provide your question."
            )
        }
        
        var messageHistory = context
        
        let instructions = [
            SYSTEM_PROMPT,
//            PLANNER_PROMPT
        ].joined(separator: "\n\n")
        
        do {
            let request = CreateModelResponseQuery(
                input: .textInput(userInput),
                model: .gpt4_o_mini,
                instructions: instructions.replacingOccurrences(of: "[ATTRIBUTE_VALUE_LIST]", with: self.attributeValueList),
                reasoning: .init(effort: .none),
                text: .text,
                toolChoice: .ToolChoiceFunction(.init(_type: .function, name: "QueryPatients")),
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
                            logger.debug("Processing query: \(queryRequest)")
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
                // Get some description fo the results
                let request = CreateModelResponseQuery(
                    input: .inputItemList(
                        [
                            .inputMessage(.init(role: .user, content: .textInput(userInput))),
                            .inputMessage(.init(role: .assistant, content: .textInput(agentResponse.proposedQuery.debugDescription))),
                            .inputMessage(.init(role: .assistant, content: .textInput(agentResponse.queryResults.debugDescription)))
                        ]
                    ),
                    model: .gpt5,
                    instructions: RESULT_FOLLOWUP,
                    reasoning: .init(effort: .none),
                    text: .text,
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
                    Components.Schemas.EventFilter(
                        attr: ef.attr,
                        value: normalizeCode(ef.value),
                        startYyyymm: ef.startYyyymm,
                        endYyyymm: ef.endYyyymm
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
    
    // MARK: - Static prompt templates
    
    /// System prompt that encodes role, plan, tool usage, persistence, and concise final answers.
    /// This mirrors patterns from the GPT‑5 Prompting Guide (tool preambles, planning, persistence,
    /// limiting eagerness, and clear stop conditions).
    
    private let SYSTEM_PROMPT = """
    You are an expert in clinical informatics expert and data analysis and reporting. Your job is extract the important conditions from the user input, arrange them logically in inclusion and exclusion conditions, and put them in a **structured** form honored by QueryPatients tool. These conditions may include the following:
    
      - Attributes (do not have time component): gender, race, ethnicity, yearOfBirth, state, metro, urban
        Here is a list of allowed attribute values:
    
    [ATTRIBUTE_VALUE_LIST]
    
      - Events: conditionCode, medicationCode, procedureCode. The event conditions can include a date range expressed as YYYYMM (year and month) start and end. For example, if the user input refers to H91 diagnosis from December 2019 to May 2023, you should put startYyyymm = 201912 and endYyyymm = 202305. ALWAYS include the date range if the user referred to it.
    
    The user input may have a mixture of codified values like ICD10 codes for diagnosis (eg. E11.*), Multum codes for drugs as well as spelled out or abbreviated medical terms. You must substitute the terms with the corresponding codes based on your best judgment. You may use wildcard '*' sign at the end of the code when there is no exact match. When a date range is provided for event conditions (eg., pateint diagnosed with apnea in 2022), make sure to convert the range to the YYYYMM format and use **anyOf** predicate along with startYyyymm and endYyyymm properties.
    
    Objectives
    - If the user intent is to query data, compose a valid QueryRequest (JSON) for the QueryPatients tool according to the tool schema.
    - Replace any 'x' or 'X' in codes with '*'. Support wildcards such as 'E11.*'.
    - If codes are not given, infer common medical codes (e.g., "type 2 diabetes" → E11.*).
    - Recognize user-provided date range and pass it as startYyyymm and endYyyymm in YYYYMM format.
    - Use allOf/anyOf/exclude correctly. If a date range or a wildcard code is provided, ALWAYS apply 'anyOf' predicate.
    - If necessary information is missing or unsupported attributes are requested, ask a **brief** clarification and/or suggest supported options.
    
    Autonomy & Stop Conditions
    - Bias toward the accurate representation of the user input in the query conditions. Make sure to capture ALL relevant information.
    - Stop after the query is executed and results are summarized with 1–2 suggested refinements.
    - Keep the final user-facing message brief and clear.
    """
    
    /// “Planner” prompt for a single turn. The model should return a **minimal plan** and either
    /// a proposed QueryRequest or a clarification.
    private let PLANNER_PROMPT = """
    Task: Given the user message and prior context, decide if you can run a query now.
    If yes: produce a full plan and a complete QueryRequest (attributes/events), then run the query.
    If no: ask for at most 2 clarifications (brief), and propose defaults where reasonable.
    - Normalize codes (x/X → *).
    - Infer common codes when users provide medical terms. Use wildcards if necessary.
    - Extract date range in YYYYMM format if the user request refers to a date range. If the user refers to the current date, use it as the basis for the date range calculation. For example, if the user asks for events in the past year, use the current date minus one year. NEVER ignore the user specified date range.
    - Use allOf/anyOf and exclude properly.
    Output: a short plan; then either (A) a QueryRequest or (B) clarifications to ask.
    """
    
    private let RESULT_FOLLOWUP = """
    You are an expert in clinical informatics expert and data analysis and reporting. You have just completed  a task of estimating a patient cohort size based on the user input.
    
    Instructions:
    - Analyze the results. If the result count is 0, it may indicate to a problem with the formulation of the request or the query. Critically review the user input and the query formulation. Provide consise recommendation and ask the user if they would like to proceed.
    - If the result count looks reasonable, suggest options to further refine the cohort definition but stay within the available attributes and events. Do NOT suggest other attributes or events that are not available in the dataset. Make suggestions brief.
    -- Available Atributes: gender, race, ethnicity, yearOfBirth, state, metro, urban.
    -- Available Events: conditionCode, medicationCode, procedureCode.
    - Politely ask if the user would like to go on with either option or if they would like to refine the query further.
    """

}



