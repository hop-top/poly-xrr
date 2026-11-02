#!/usr/bin/env bats
# The suite calls `az` and `open` like any script would. Under `xrr run`,
# `az` is answered from cassettes and `open` is blocked.

@test "account show returns the subscription" {
  run az account show
  [ "$status" -eq 0 ]
  [[ "$output" == *'"name":"contoso"'* ]]
}

@test "group list returns both groups" {
  run az group list
  [ "$status" -eq 0 ]
  [[ "$output" == *rg-data* ]]
}

@test "unknown subcommand surfaces the recorded exit code" {
  run az frobnicate
  [ "$status" -eq 2 ]
}
