<!--
Is there any breaking changes?  If so this is a major release, make sure '#major' is in at least one
commit message to get CI to bump the major.  This will prevent automatic down stream dependency
bumping / consuming.  For more information about semantic versioning see: https://semver.org/


Suggested PR template: Fill/delete/add sections as needed. Optionally delete any commented block.
-->
What
----
<!--
Briefly describe **what** you have changed and **why**.
Optionally include implementation strategy.
-->

Checklist
------------------
Please answer the questions with Y, N or N/A if not applicable.
- **[ ]** Contains customer facing changes? Including API/behavior changes <!-- This can help identify if it has introduced any breaking changes -->
- **[ ]** Is this change gated behind config(s)?
    - List the config(s) needed to be set to enable this change
- **[ ]** Did you add sufficient unit test and/or integration test coverage for this PR?
    - If not, please explain why it is not required
- **[ ]** Does this change require modifying existing system tests or adding new system tests? <!-- Primarily for changes that could impact CCloud integrations -->
    - If so, include tracking information for the system test changes
- **[ ]** Must this be released together with other change(s), either in this repo or another one?
    - If so, please include the link(s) to the changes that must be released together

References
----------
JIRA:
<!--
Copy&paste links: to Jira ticket, other PRs, issues, Slack conversations...
For code bumps: link to PR, tag or GitHub `/compare/master...master`
-->

Test & Review
------------
<!--
Has it been tested? how?
Copy&paste any handy instructions, steps or requirements that can save time to the reviewer or any reader.
-->

Open questions / Follow-ups
--------------------------
<!--
Optional: anything open to discussion for the reviewer, out of scope, or follow ups.
-->

<!--
Review stakeholders
------------------
<!--
Optional: mention stakeholders or if special context that is required to review.
-->
