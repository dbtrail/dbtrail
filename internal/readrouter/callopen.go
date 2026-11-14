package readrouter

// callOpen ends a pattern that finds a function's name followed by the
// parenthesis that opens its arguments: the white space MySQL takes between
// the two, then the parenthesis. MySQL's lexer reads a space and the bytes
// 0x09 to 0x0D (a tab, a line feed, a vertical tab, a form feed, a carriage
// return) as white space, and refuses every other byte below 0x20 there
// (measured on MySQL 8.0.46, #2802). Go's \s leaves out the vertical tab: a
// pattern ending in \s*\( does not see DATEDIFF\v(a, b) as a call, where
// MySQL runs it and callArgs, which the rewrites for the copy read the
// arguments with, skips the vertical tab too. The control then lets through
// a call the rewrite still sends to the copy.
//
// Whether MySQL takes white space there at all depends on the function:
// without IGNORE_SPACE in sql_mode, NOW () and COUNT (*) are errors and
// DATEDIFF (a, b) is not. A pattern that finds a call MySQL refuses keeps
// the statement on MySQL, which then refuses it.
const callOpen = `[\s\v]*\(`
