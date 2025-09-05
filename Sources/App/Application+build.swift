import Hummingbird
import Foundation
import Logging
import OpenAPIHummingbird

/// Application arguments protocol. We use a protocol so we can call
/// `buildApplication` inside Tests as well as in the App executable. 
/// Any variables added here also have to be added to `App` in App.swift and 
/// `TestArguments` in AppTest.swift
public protocol AppArguments {
    var hostname: String { get }
    var port: Int { get }
    var logLevel: Logger.Level? { get }
}

// Request context used by application
typealias AppRequestContext = BasicRequestContext

enum AppErrors: Error {
    case invalidIndexFilePath
}

///  Build application
/// - Parameter arguments: application arguments
public func buildApplication(_ arguments: some AppArguments) async throws -> some ApplicationProtocol {
    let environment = Environment()
    let logger = {
        var logger = Logger(label: "rwdqueryservice")
        logger.logLevel = 
            arguments.logLevel ??
            environment.get("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ??
            .info
        return logger
    }()
    let router = try buildRouter()
    
    // add query engine and load the index
    let queryEngine = QueryEngine(logger: logger)
    guard let indexFilePath = environment.get("INDEX_FILE_PATH"),
          let indexFileUrl = URL(string: indexFilePath) else {
        logger.error("INDEX_FILE_PATH environment variable is invalid")
        throw AppErrors.invalidIndexFilePath
    }
    logger.info("Loading index")
    try queryEngine.loadIndex(from: indexFileUrl)
    logger.info("Loading index complete")
    
    // Register Query Engine and add OpenAPI handlers
    let api = APIImplementation(queryEngine: queryEngine)
    try api.registerHandlers(on: router)

    let app = Application(
        router: router,
        configuration: .init(
            address: .hostname(arguments.hostname, port: arguments.port),
            serverName: "rwdqueryservice"
        ),
        logger: logger
    )
    return app
}

/// Build router
func buildRouter() throws -> Router<AppRequestContext> {
    let router = Router(context: AppRequestContext.self)
    // Add middleware
    router.addMiddleware {
        // logging middleware
        LogRequestsMiddleware(.info)
        // static files
        FileMiddleware("public/swagger-ui", urlBasePath: "/docs", searchForIndexHtml: true)
        // store request context in TaskLocal
        OpenAPIRequestContextMiddleware()
    }
    
    router.add(middleware: CORSMiddleware(
        allowOrigin: .all,
        allowHeaders: [.accept, .contentType, .authorization],
        allowMethods: [.get, .post],
        allowCredentials: true,
        maxAge: .seconds(3600)
    ))
        
    return router
}

