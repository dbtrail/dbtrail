package console

// SetBcryptCostForTest sets the cost the console hashes passwords at and
// returns a function that puts back the previous one. Test helper ONLY: a test
// binary lowers the cost so password logins stop dominating its run time. No
// shipped code may call it; TestBcryptCostHookIsTestOnly fails the build if
// any non-test file does, and TestShippedBcryptCost pins the shipped value.
func SetBcryptCostForTest(cost int) (restore func()) {
	prev := bcryptCost
	bcryptCost = cost
	return func() { bcryptCost = prev }
}
