//
//  OCISignerMiddleware.swift
//  rwdqueryservice
//
//  Created by Ilia Sazonov on 9/10/25.
//

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

import OpenAI
import OCIKit
import Logging

public final class OCISignerMiddleware: OpenAIMiddleware,  @unchecked Sendable {
    let signer: Signer
    let logger: Logger
    
    public init(signer: Signer, logger: Logger) {
        self.signer = signer
        self.logger = logger
    }
    
    public func intercept(request: URLRequest) -> URLRequest {
        var signedRequest = request
        if request.httpBody != nil {
            do {
                try signer.sign(&signedRequest)
            } catch {
                logger.error("Unable to sign the request\n\(error.localizedDescription)")
            }
        }
        return signedRequest
    }
    
//    public func interceptStreamingData(request: URLRequest?, _ data: Data) -> Data {
//        return data
//    }
    
//    public func intercept(response: URLResponse?, request: URLRequest, data: Data?) -> (response: URLResponse?, data: Data?) {
//        if let response {
//            print("response: \(response)")
//        }
//        
//        if let data {
//            print("resposne data string: \(String(data: data, encoding: .utf8) ?? "nil")")
//        }
//        return (response, data)
//    }
}
