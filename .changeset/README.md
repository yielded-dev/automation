# Changesets

Automation and Sandbox share a stable version through a Changesets fixed group.
Add a changeset for consumer-visible changes with `vp run changeset`, naming the
affected packages. Changesets versions both public packages together; private
examples and the workspace root are not versioned or published.

Review the plan with `vp run changeset status`. Main updates the automated
`chore: version packages` PR; merging that PR publishes the reviewed versions
after the release checks pass. See [release setup](../README.md#releases).
