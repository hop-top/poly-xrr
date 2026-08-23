<?php

declare(strict_types=1);

namespace HopTop\Xrr;

use HopTop\Xrr\Exception\CassetteMissException;
use HopTop\Xrr\Stream\OccurrenceCounter;

class Session
{
    private ?OccurrenceCounter $streamOccurrences = null;

    public function __construct(
        private Mode $mode,
        private FileCassette $cassette
    ) {}

    /**
     * Occurrence counter for streamed opens whose fingerprint carries `n`.
     * One session object is one counter domain; record and replay count
     * identically (cassette-format-streaming.md, Fingerprinting).
     */
    public function streamOccurrences(): OccurrenceCounter
    {
        return $this->streamOccurrences ??= new OccurrenceCounter();
    }

    /**
     * Execute one interaction according to session mode.
     *
     * record:      calls $do(), saves to cassette, returns result.
     * replay:      loads from cassette, returns deserialized resp; never calls $do().
     * passthrough: calls $do(), never touches cassette.
     *
     * @throws CassetteMissException on replay miss
     */
    public function record(AdapterInterface $adapter, mixed $req, callable $do): mixed
    {
        return match ($this->mode) {
            Mode::Record      => $this->doRecord($adapter, $req, $do),
            Mode::Replay      => $this->doReplay($adapter, $req),
            Mode::Passthrough => $do($req),
        };
    }

    private function doRecord(AdapterInterface $adapter, mixed $req, callable $do): mixed
    {
        $resp = $do($req);

        $fp = $adapter->fingerprint($req);
        $this->cassette->save(
            $adapter->getId(),
            $fp,
            $adapter->serializeReq($req),
            $adapter->serializeResp($resp)
        );

        return $resp;
    }

    private function doReplay(AdapterInterface $adapter, mixed $req): mixed
    {
        $fp   = $adapter->fingerprint($req);
        $data = $this->cassette->load($adapter->getId(), $fp);

        return $adapter->deserializeResp($data['resp']);
    }
}
