// swift-tools-version:6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.
// Adjusted tools version to match installed Swift 6.0.3. If you upgrade Xcode/Swift to 6.1+, revert to 6.1.

import PackageDescription

let package = Package(
    name: "rwdqueryservice",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .executable(name: "App", targets: ["App"]),
    ],
    dependencies: [
//        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.0.0"),
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.3.0"),
        .package(url: "https://github.com/apple/swift-openapi-generator", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-openapi-runtime", from: "1.7.0"),
        .package(url: "https://github.com/swift-server/swift-openapi-hummingbird.git", from: "2.0.1"),
        .package(url: "https://github.com/apple/swift-algorithms", from: "1.2.1"),
        .package(url: "https://github.com/RoaringBitmap/SwiftRoaring",  branch: "master"),
        .package(url: "https://github.com/apple/swift-numerics", from: "1.1.0"),
    ],
    targets: [
        .executableTarget(name: "App",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "Hummingbird", package: "hummingbird"),
                .byName(name: "AppAPI"),
                .product(name: "OpenAPIRuntime", package: "swift-openapi-runtime"),
                .product(name: "OpenAPIHummingbird", package: "swift-openapi-hummingbird"),
                .product(name: "Algorithms", package: "swift-algorithms"),
                .product(name: "Numerics", package: "swift-numerics"),
                .product(name: "SwiftRoaring", package: "SwiftRoaring"),
            ],
            path: "Sources/App"
        ),
        .target(
            name: "AppAPI",
            dependencies: [
                .product(name: "OpenAPIRuntime", package: "swift-openapi-runtime")
            ],
            path: "Sources/AppAPI",
            plugins: [.plugin(name: "OpenAPIGenerator", package: "swift-openapi-generator")]
        ),
        .testTarget(name: "AppTests",
            dependencies: [
                .byName(name: "App"),
                .product(name: "HummingbirdTesting", package: "hummingbird")
            ],
            path: "Tests/AppTests"
        )
    ]
)
