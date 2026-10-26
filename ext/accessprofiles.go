package ext

// accessProfilesVisible is whether the access-profiles authoring surfaces are
// advertised. Off in the stock build.
var accessProfilesVisible bool

// SetAccessProfilesVisible advertises the access-profiles authoring surfaces:
// the console's Access profiles page (nav entry, command-palette entry, route)
// and the flag/profile/access commands in CLI help. Off by default, because a
// stock install has one operator and nothing that assigns a profile to anyone,
// so the page explains a mechanism with no one to apply it to. An embedding
// build that assigns profiles to signed-in users turns it on at startup.
//
// It hides; it does not disable. The /api/access-profiles routes, the CLI verbs
// and every --profile flag keep working, and rules already in an index keep
// being enforced. Turning visibility off must never unmask data a rule hides.
//
// Startup-only, like the other ext setters: call it from main() before the
// console or the CLI starts, never while serving.
func SetAccessProfilesVisible(on bool) { accessProfilesVisible = on }

// AccessProfilesVisible reports what SetAccessProfilesVisible last set.
func AccessProfilesVisible() bool { return accessProfilesVisible }
