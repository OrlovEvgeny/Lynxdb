package main

// Standard exit codes for the LynxDB CLI.
const (
	exitOK           = 0   // Command completed successfully.
	exitGeneral      = 1   // Unspecified failure.
	exitUsage        = 2   // Invalid flags or missing arguments.
	exitConnection   = 3   // Cannot reach server.
	exitQueryParse   = 4   // Bad SPL2 syntax.
	exitQueryTimeout = 5   // Server timeout or --timeout exceeded.
	exitNoResults    = 6   // Query returned 0 results (with --fail-on-empty).
	exitAuth         = 7   // Missing or invalid authentication token.
	exitAborted      = 10  // User declined destructive action confirmation.
	exitTimeout      = 124 // Generic timeout (GNU convention).
	exitInterrupted  = 130 // User pressed Ctrl+C (SIGINT).
)
