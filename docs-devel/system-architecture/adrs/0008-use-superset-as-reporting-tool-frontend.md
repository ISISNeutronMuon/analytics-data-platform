# 8. Use Superset as frontend replacement of reporting-tool

Date: 2026-05-21

## Status

Accepted

## Context

Operational data from the FASE application is extracted and loaded into a central lakehouse environment. To enable reporting and data analysis against this lakehouse, we are exploring options for a new frontend BI tool. As part of this evaluation, we are assessing two popular open-source solutions:

- [Superset](https://superset.apache.org/)
- [Metabase](https://www.metabase.com/)

## Decision

We will evaluate both Apache Superset and Metabase as potential frontend replacements for the legacy reporting tool. This evaluation will be conducted locally across the following criteria to determine the best fit for our architecture:

- Data Modeling & Representation: Assess and compare how each tool models and presents lakehouse data relative to the current reporting tool.
- Visualization & Charting Capabilities: Evaluate the chart types, formatting options, and visualization parity with existing reports.
- Access Control (RBAC): Analyze role-based access controls and granular data permissioning features in both platforms.
- User Authentication: Study integration capabilities with existing authentication providers (e.g., OIDC, SAML, or LDAP).

## Consequences

While both Apache Superset and Metabase are strong open-source candidates with robust data modeling capabilities, our organization's authentication requirements heavily favor Superset.

- Security & Identity Alignment: Our analytics data platform relies strictly on a centralized identity service. Metabase Open Source only supports LDAP, API keys, and Google OAuth, which does not align with our internal authentication standards. Superset natively supports our required centralized identity integration (e.g., OIDC/SAML).
- Feature Parity: Superset delivers reporting and data visualization capabilities comparable to Metabase, ensuring we do not compromise on core BI functionality.

Final Outcome: Choosing Superset ensures full compliance with our security architecture without sacrificing data representation features, making it the preferred long-term solution.
