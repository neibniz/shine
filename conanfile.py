from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout


class ShineProxyConan(ConanFile):
    name = "shine"
    version = "0.1.0"
    license = "MIT"
    description = "Shine proxy: TCP -> RESP2 -> shine multiplex proxy"
    settings = "os", "compiler", "build_type", "arch"
    options = {"with_tests": [True, False]}
    default_options = {"with_tests": True}

    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "src/*",
        "include/*",
        "tests/*",
        "configs/*",
    )

    def requirements(self):
        self.requires("boost/1.85.0")
        self.requires("zlib/1.3.1")
        self.requires("abseil/20240116.2")
        self.requires("yaml-cpp/0.8.0")
        self.requires("prometheus-cpp/1.2.4")
        self.requires("protobuf/3.21.12")
        self.requires("spdlog/1.15.1")
        self.requires("fmt/11.1.4", override=True)

    def build_requirements(self):
        if self.options.with_tests:
            self.test_requires("gtest/1.14.0")

    def configure(self):
        self.options["boost"].without_python = True
        self.options["boost"].without_wave = True
        self.options["boost"].without_graph = True
        self.options["boost"].without_iostreams = True
        self.options["boost"].without_locale = True
        self.options["boost"].without_log = True
        self.options["boost"].without_mpi = True
        self.options["boost"].without_nowide = True
        self.options["boost"].without_serialization = True
        self.options["boost"].without_stacktrace = True
        self.options["boost"].without_test = True
        self.options["boost"].without_type_erasure = True

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["SHINE_WITH_TESTS"] = "ON" if self.options.with_tests else "OFF"
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
