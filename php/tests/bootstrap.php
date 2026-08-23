<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

/*
 * The `Grpc\OP_*` batch opcodes are registered by the ext-grpc C extension,
 * but grpc-php's call classes reference them from plain PHP. Defining them
 * when the extension is absent lets the adapter's record-side logic be
 * exercised against a scripted batch double on a machine without ext-grpc.
 *
 * Values are the `grpc_op_type` enum from grpc/impl/grpc_types.h, which is
 * ABI-stable. When the extension IS loaded these definitions are skipped
 * entirely, so the real constants always win.
 */
if (!defined('Grpc\OP_SEND_INITIAL_METADATA')) {
    define('Grpc\OP_SEND_INITIAL_METADATA', 0);
    define('Grpc\OP_SEND_MESSAGE', 1);
    define('Grpc\OP_SEND_CLOSE_FROM_CLIENT', 2);
    define('Grpc\OP_SEND_STATUS_FROM_SERVER', 3);
    define('Grpc\OP_RECV_INITIAL_METADATA', 4);
    define('Grpc\OP_RECV_MESSAGE', 5);
    define('Grpc\OP_RECV_STATUS_ON_CLIENT', 6);
    define('Grpc\OP_RECV_CLOSE_ON_SERVER', 7);
}
