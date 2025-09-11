//
//  LoggingMiddleware.swift
//  rwdqueryservice
//
//  Created by Ilia Sazonov on 9/10/25.
//

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

import OpenAI

public final class LoggingMiddleware: OpenAIMiddleware {
    public init() {}
    
    public func intercept(request: URLRequest) -> URLRequest {
        if let body = request.httpBody {
            if
                let jsonObject = try? JSONSerialization.jsonObject(with: body, options: []),
                let prettyData = try? JSONSerialization.data(withJSONObject: jsonObject, options: [.prettyPrinted]),
                let prettyString = String(data: prettyData, encoding: .utf8)
            {
                print("request body (pretty):\n\(prettyString)")
            } else if let raw = String(data: body, encoding: .utf8) {
                print("request body:\n\(raw)")
            } else {
                print("request body: <non-UTF8 data: \(body.count) bytes>")
            }
        } else {
            print("request body: nil")
        }
        return request
    }
    
    public func interceptStreamingData(request: URLRequest?, _ data: Data) -> Data {
        return data
    }
    
    public func intercept(response: URLResponse?, request: URLRequest, data: Data?) -> (response: URLResponse?, data: Data?) {
        if let response {
            print("response: \(response)")
        }
        
        if let data {
            print("resposne data string: \(String(data: data, encoding: .utf8) ?? "nil")")
        }
        return (response, data)
    }
}
