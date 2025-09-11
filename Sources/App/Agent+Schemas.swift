import Foundation
import OpenAI
import AppAPI
import Yams

extension AppAPI.Components.Schemas.AttrVal.AttrPayload: JSONSchemaEnumConvertible {
    public var caseNames: [String] { Self.allCases.map { $0.rawValue } }
}

extension AppAPI.Components.Schemas.EventFilter.AttrPayload: JSONSchemaEnumConvertible {
    public var caseNames: [String] { Self.allCases.map { $0.rawValue } }
}

func derivedQueryRequestSchema() throws -> JSONSchema {
    let def: JSONSchemaDefinition = .derivedJsonSchema(AppAPI.Components.Schemas.QueryRequest.self)
    let data = try JSONEncoder().encode(def)
    let derived = try JSONDecoder().decode(JSONSchema.self, from: data)
    return try enrichQueryRequestDescriptions(fromOpenAPI: derived)
}

func derivedQueryRequestSchemaJSONString(prettyPrinted: Bool = true) throws -> String {
    let schema = try derivedQueryRequestSchema()
    let encoder = JSONEncoder()
    if prettyPrinted {
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    } else {
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    }
    let data = try encoder.encode(schema)
    return String(decoding: data, as: UTF8.self)
}

let queryRequestJsonSchemaString = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "QueryRequest",
  "type": "object",
  "description": "Structured query specifying attribute and event filters.",
  "properties": {
    "attributes": {
      "type": "object",
      "description": "Timeless patient attribute filters.",
      "properties": {
        "allOf": {
          "type": "array",
          "description": "Patients must have all of these attributes.",
          "items": {
            "type": "object",
            "description": "Attribute filter specifying an attribute-value pair.",
            "properties": {
              "attr": {
                "type": "string",
                "enum": [
                  "gender",
                  "race",
                  "ethnicity",
                  "yearOfBirth",
                  "state",
                  "metro",
                  "urban"
                ]
              },
              "value": {
                "type": "string",
                "description": "Attribute value."
              }
            },
            "required": ["attr", "value"]
          }
        },
        "anyOf": {
          "type": "array",
          "description": "Patients must have at least one of these attributes.",
          "items": {
            "$ref": "#/properties/attributes/properties/allOf/items"
          }
        },
        "exclude": {
          "type": "array",
          "description": "Patients must not have any of these attributes.",
          "items": {
            "$ref": "#/properties/attributes/properties/allOf/items"
          }
        }
      }
    },
    "events": {
      "type": "object",
      "description": "Event filters with optional time windows.",
      "properties": {
        "allOf": {
          "type": "array",
          "description": "Patients must have all of these events.",
          "items": {
            "type": "object",
            "description": "Event filter specifying an event type, value, and optional time window.",
            "properties": {
              "attr": {
                "type": "string",
                "enum": ["conditionCode", "medicationCode", "procedureCode"]
              },
              "value": {
                "type": "string",
                "description": "Event code or value (supports wildcards like H91.1*)."
              },
              "start_yyyymm": {
                "type": "integer",
                "description": "Optional start date in yyyymm format (e.g., 202104)."
              },
              "end_yyyymm": {
                "type": "integer",
                "description": "Optional end date in yyyymm format (e.g., 202405)."
              }
            },
            "required": ["attr", "value"],
            "dependentRequired": {
              "start_yyyymm": ["end_yyyymm"],
              "end_yyyymm": ["start_yyyymm"]
            }
          }
        },
        "anyOf": {
          "type": "array",
          "description": "Patients must have at least one of these events.",
          "items": {
            "$ref": "#/properties/events/properties/allOf/items"
          }
        },
        "exclude": {
          "type": "array",
          "description": "Patients must not have any of these events.",
          "items": {
            "$ref": "#/properties/events/properties/allOf/items"
          }
        }
      }
    }
  }
}
"""

func getQueryRequestSchema(from jsonSchemaString: String) throws -> JSONSchema {
    let data = Data(jsonSchemaString.utf8)
    let schema = try JSONDecoder().decode(JSONSchema.self, from: data)
    return schema
}

extension AppAPI.Components.Schemas.QueryRequest: JSONSchemaConvertible {
    package static var example: AppAPI.Components.Schemas.QueryRequest {
        let attrAllOf: [AppAPI.Components.Schemas.AttrVal] = [
            .init(attr: .gender, value: "Male"),
            .init(attr: .ethnicity, value: "Hispanic or Latino")
        ]
        let attrAnyOf: [AppAPI.Components.Schemas.AttrVal] = [
            .init(attr: .race, value: "Asian"),
            .init(attr: .race, value: "Black or African American")
        ]
        let attrExclude: [AppAPI.Components.Schemas.AttrVal] = [
            .init(attr: .urban, value: "urban")
        ]
        let attributes = AppAPI.Components.Schemas.AttributeFilters(
            allOf: attrAllOf,
            anyOf: attrAnyOf,
            exclude: attrExclude
        )

        // Use a canonical full-range window for items that previously omitted dates,
        // so no optionals are nil in the example.
        let fullRangeStart = 200001
        let fullRangeEnd = 209912

        let eventsAnyOf: [AppAPI.Components.Schemas.EventFilter] = [
            .init(attr: .conditionCode, value: "E11.*", startYyyymm: fullRangeStart, endYyyymm: fullRangeEnd),
            .init(attr: .procedureCode, value: "0TY00Z0", startYyyymm: fullRangeStart, endYyyymm: fullRangeEnd)
        ]

        let eventsAllOf: [AppAPI.Components.Schemas.EventFilter] = [
            .init(attr: .medicationCode, value: "LENVATINIB", startYyyymm: 202001, endYyyymm: 202312),
            .init(attr: .conditionCode, value: "I10.*", startYyyymm: fullRangeStart, endYyyymm: fullRangeEnd)
        ]

        let eventsExclude: [AppAPI.Components.Schemas.EventFilter] = [
            .init(attr: .conditionCode, value: "C50.*", startYyyymm: fullRangeStart, endYyyymm: fullRangeEnd),
            .init(attr: .procedureCode, value: "30233N1", startYyyymm: 201001, endYyyymm: 201512)
        ]

        let events = AppAPI.Components.Schemas.EventFilters(
            allOf: eventsAllOf,
            anyOf: eventsAnyOf,
            exclude: eventsExclude
        )

        return .init(attributes: attributes, events: events)
    }
}

let queryRequestExample = """
{
  "attributes": {
    "allOf": [
      { "attr": "gender", "value": "Male" },
      { "attr": "ethnicity", "value": "Hispanic or Latino" }
    ],
    "anyOf": [
      { "attr": "race", "value": "Asian" },
      { "attr": "race", "value": "Black or African American" }
    ],
    "exclude": [
      { "attr": "urban", "value": "urban" }
    ]
  },
  "events": {
    "anyOf": [
      {
        "attr": "conditionCode",
        "value": "E11.*",
      },
      {
        "attr": "procedureCode",
        "value": "0TY00Z0"
      }
    ],
    "allOf": [
      {
        "attr": "medicationCode",
        "value": "LENVATINIB",
        "start_yyyymm": 202001,
        "end_yyyymm": 202312
      },
      {
        "attr": "conditionCode",
        "value": "I10.*"
      }
    ],
    "exclude": [
      {
        "attr": "conditionCode",
        "value": "C50.*"
      },
      {
        "attr": "procedureCode",
        "value": "30233N1", 
        "start_yyyymm": 201001,
        "end_yyyymm": 201512
      }
    ]
  }
}
"""


private func enrichQueryRequestDescriptions(fromOpenAPI schema: JSONSchema) throws -> JSONSchema {
    guard case var .object(root) = schema else { return schema }
    guard let openAPI = try loadOpenAPIObject() else { return schema }

    func desc(_ path: [String]) -> String? {
        value(at: path, in: openAPI) as? String
    }

    if let rootTitle = value(at: ["components","schemas","QueryRequest","title"], in: openAPI) as? String {
        setKey(in: &root, path: [], key: "title", value: AnyJSONDocument(rootTitle))
    }
    if let rootSchema = value(at: ["components","schemas","QueryRequest","$schema"], in: openAPI) as? String {
        setKey(in: &root, path: [], key: "$schema", value: AnyJSONDocument(rootSchema))
    } else {
        setKey(in: &root, path: [], key: "$schema", value: AnyJSONDocument("https://json-schema.org/draft/2020-12/schema"))
    }
    if let rootDescription = desc(["components","schemas","QueryRequest","description"]) {
        setDescription(in: &root, path: [], description: rootDescription)
    }

    if let attributesDesc = desc(["components","schemas","AttributeFilters","description"]) {
        setDescription(in: &root, path: ["properties","attributes"], description: attributesDesc)
    }
    if let attrAllOfDesc = desc(["components","schemas","AttributeFilters","properties","allOf","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","allOf"], description: attrAllOfDesc)
    }
    if let attrAnyOfDesc = desc(["components","schemas","AttributeFilters","properties","anyOf","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","anyOf"], description: attrAnyOfDesc)
    }
    if let attrExcludeDesc = desc(["components","schemas","AttributeFilters","properties","exclude","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","exclude"], description: attrExcludeDesc)
    }

    if let attrItemDesc = desc(["components","schemas","AttrVal","description"]) ?? desc(["components","schemas","AttributeFilters","properties","allOf","items","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","allOf","items"], description: attrItemDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","anyOf","items"], description: attrItemDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","exclude","items"], description: attrItemDesc)
    }
    if let attrNameDesc = desc(["components","schemas","AttrVal","properties","attr","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","allOf","items","properties","attr"], description: attrNameDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","anyOf","items","properties","attr"], description: attrNameDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","exclude","items","properties","attr"], description: attrNameDesc)
    }
    if let attrValueDesc = desc(["components","schemas","AttrVal","properties","value","description"]) {
        setDescription(in: &root, path: ["properties","attributes","properties","allOf","items","properties","value"], description: attrValueDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","anyOf","items","properties","value"], description: attrValueDesc)
        setDescription(in: &root, path: ["properties","attributes","properties","exclude","items","properties","value"], description: attrValueDesc)
    }

    if let eventsDesc = desc(["components","schemas","EventFilters","description"]) {
        setDescription(in: &root, path: ["properties","events"], description: eventsDesc)
    }
    if let evtAllOfDesc = desc(["components","schemas","EventFilters","properties","allOf","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf"], description: evtAllOfDesc)
    }
    if let evtAnyOfDesc = desc(["components","schemas","EventFilters","properties","anyOf","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","anyOf"], description: evtAnyOfDesc)
    }
    if let evtExcludeDesc = desc(["components","schemas","EventFilters","properties","exclude","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","exclude"], description: evtExcludeDesc)
    }

    if let eventFilterItemDesc = desc(["components","schemas","EventFilter","description"]) ?? desc(["components","schemas","EventFilters","properties","allOf","items","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf","items"], description: eventFilterItemDesc)
        setDescription(in: &root, path: ["properties","events","properties","anyOf","items"], description: eventFilterItemDesc)
        setDescription(in: &root, path: ["properties","events","properties","exclude","items"], description: eventFilterItemDesc)
    }
    if let evtAttrDesc = desc(["components","schemas","EventFilter","properties","attr","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf","items","properties","attr"], description: evtAttrDesc)
        setDescription(in: &root, path: ["properties","events","properties","anyOf","items","properties","attr"], description: evtAttrDesc)
        setDescription(in: &root, path: ["properties","events","properties","exclude","items","properties","attr"], description: evtAttrDesc)
    }
    if let evtValueDesc = desc(["components","schemas","EventFilter","properties","value","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf","items","properties","value"], description: evtValueDesc)
        setDescription(in: &root, path: ["properties","events","properties","anyOf","items","properties","value"], description: evtValueDesc)
        setDescription(in: &root, path: ["properties","events","properties","exclude","items","properties","value"], description: evtValueDesc)
    }
    if let startDesc = desc(["components","schemas","EventFilter","properties","start_yyyymm","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf","items","properties","startYyyymm"], description: startDesc)
        setDescription(in: &root, path: ["properties","events","properties","anyOf","items","properties","startYyyymm"], description: startDesc)
        setDescription(in: &root, path: ["properties","events","properties","exclude","items","properties","startYyyymm"], description: startDesc)
    }
    if let endDesc = desc(["components","schemas","EventFilter","properties","end_yyyymm","description"]) {
        setDescription(in: &root, path: ["properties","events","properties","allOf","items","properties","endYyyymm"], description: endDesc)
        setDescription(in: &root, path: ["properties","events","properties","anyOf","items","properties","endYyyymm"], description: endDesc)
        setDescription(in: &root, path: ["properties","events","properties","exclude","items","properties","endYyyymm"], description: endDesc)
    }

    return .object(root)
}

// YAML loading and traversal

private func loadOpenAPIObject() throws -> [String: Any]? {
    let fm = FileManager.default
    let cwd = fm.currentDirectoryPath
    let candidates = [
        ProcessInfo.processInfo.environment["OPENAPI_SPEC_PATH"],
        "\(cwd)/Sources/AppAPI/openapi.yaml",
        "\(cwd)/public/swagger-ui/openapi.yaml"
    ].compactMap { $0 }
    for path in candidates {
        if fm.fileExists(atPath: path) {
            let yaml = try String(contentsOfFile: path, encoding: .utf8)
            if let any = try Yams.load(yaml: yaml) as? [String: Any] {
                return any
            }
        }
    }
    return nil
}

private func value(at path: [String], in dict: [String: Any]) -> Any? {
    var node: Any? = dict
    for key in path {
        guard let d = node as? [String: Any] else { return nil }
        node = d[key]
    }
    return node
}

// Reuse helper functions from earlier enrichment

private let _jsonEncoder = JSONEncoder()
private let _jsonDecoder = JSONDecoder()

private func asObject(_ doc: AnyJSONDocument) -> [String: AnyJSONDocument]? {
    guard let data = try? _jsonEncoder.encode(doc) else { return nil }
    return try? _jsonDecoder.decode([String: AnyJSONDocument].self, from: data)
}

private func makeObject(_ obj: [String: AnyJSONDocument]) -> AnyJSONDocument {
    AnyJSONDocument(obj)
}

private func setDescription(in dict: inout [String: AnyJSONDocument], path: [String], description: String) {
    setKey(in: &dict, path: path, key: "description", value: AnyJSONDocument(description))
}

private func setKey(in dict: inout [String: AnyJSONDocument], path: [String], key: String, value: AnyJSONDocument) {
    guard !path.isEmpty else {
        dict[key] = value
        return
    }
    let head = path[0]
    let tail = Array(path.dropFirst())
    if let existing = dict[head], var child = asObject(existing) {
        setKey(in: &child, path: tail, key: key, value: value)
        dict[head] = makeObject(child)
    }
}