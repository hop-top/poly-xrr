"""Generated gRPC stubs for the real-server round-trip tests.

Regenerate after editing xrrtest.proto:

    uv run python -m grpc_tools.protoc -I tests/grpcfixture \
        --python_out=tests/grpcfixture --grpc_python_out=tests/grpcfixture \
        tests/grpcfixture/xrrtest.proto

then re-apply the package-relative import in xrrtest_pb2_grpc.py
(protoc emits a flat `import xrrtest_pb2`).
"""
