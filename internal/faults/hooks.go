//go:build !faultinject

// Package faults provides named boundaries only in explicit faultinject builds.
package faults

const Enabled = false

// Hit is compiled out of normal release builds.
func Hit(point, identity string) {}
