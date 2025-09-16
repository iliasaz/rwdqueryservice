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
    case invalidOpenAIKeyPath
    case invalidMultumMapFilePath
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
    
    // Load drug map
    guard let multumMapFilePath = environment.get("MULTUM_MAP_FILE_PATH") else {
        logger.error("MULTUM_MAP_FILE_PATH environment variable is invalid")
        throw AppErrors.invalidMultumMapFilePath
    }
    let multumMapFileUrl = URL(fileURLWithPath: multumMapFilePath)
    logger.info("Loading Multum Map")
    try queryEngine.loadMultumMap(from: multumMapFileUrl)
    logger.info("Loading Multum Map complete")
    
    // add Agent
    let openaiApiKey: String
    if let envOpenaiKey = environment.get("OPENAI_API_KEY") {
        openaiApiKey = envOpenaiKey
    } else {
        logger.error("OPENAI_API_KEY environment variable not found")
         openaiApiKey = "<not used>"
//        throw AppErrors.invalidOpenAIKeyPath
    }
    
    // Register Query Engine, Agent, and add OpenAPI handlers
    let api = APIImplementation(queryEngine: queryEngine, openaiKey: openaiApiKey, logger: logger)
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
        // CORS
        CORSMiddleware(
            allowOrigin: .all,
            allowHeaders: [.accept, .contentType, .authorization],
            allowMethods: [.get, .post],
            allowCredentials: true,
            maxAge: .seconds(3600)
        )
        // store request context in TaskLocal
        OpenAPIRequestContextMiddleware()
    }
    
    router.get("/") { request, context -> Response in
        // This will redirect to "/docs/" with a 308 Permanent Redirect status
        return Response.redirect(to: "/docs/", type: .permanent)
    }
    
    return router
}
