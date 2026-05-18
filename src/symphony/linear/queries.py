"""Hand-rolled GraphQL strings.

Verified against Linear's introspectable schema (see iteration 7 of
`docs/linear-integration-research.md`). Variable types matter:

- `$id: String!` accepts both UUIDs and identifiers ("ENG-123") for `issue`
  and `issueUpdate`. `commentCreate.input.issueId` requires the UUID form.
- `$after: DateTimeOrDuration!` is non-null. Linear's current comment
  filter schema rejects `DateTime!` even when the value is an absolute
  RFC3339 timestamp.
"""

LOOKUP_ISSUE = """
query LookupIssue($id: String!) {
  issue(id: $id) {
    id
    identifier
    title
    description
    url
    updatedAt
    state { id name type }
    team { id key }
    labels { nodes { name } }
    relations(first: 50, includeArchived: true) {
      pageInfo { hasNextPage endCursor }
      nodes {
        type
        relatedIssue {
          id identifier archivedAt
          state { type }
        }
      }
    }
    inverseRelations(first: 50, includeArchived: true) {
      pageInfo { hasNextPage endCursor }
      nodes {
        type
        issue {
          id identifier archivedAt
          state { type }
        }
      }
    }
  }
}
"""

ISSUES_IN_STATE = """
query IssuesInState($team: String!, $stateName: String!, $label: String) {
  issues(
    filter: {
      team: { key: { eq: $team } },
      state: { name: { eq: $stateName } },
      labels: { name: { eq: $label } }
    },
    first: 50
  ) {
    nodes {
      id identifier title description url updatedAt
      state { id name type }
      team { id key }
      labels { nodes { name } }
      relations(first: 50, includeArchived: true) {
        pageInfo { hasNextPage endCursor }
        nodes {
          type
          relatedIssue {
            id identifier archivedAt
            state { type }
          }
        }
      }
      inverseRelations(first: 50, includeArchived: true) {
        pageInfo { hasNextPage endCursor }
        nodes {
          type
          issue {
            id identifier archivedAt
            state { type }
          }
        }
      }
    }
  }
}
"""

ISSUES_IN_STATE_NO_LABEL = """
query IssuesInState($team: String!, $stateName: String!) {
  issues(
    filter: {
      team: { key: { eq: $team } },
      state: { name: { eq: $stateName } }
    },
    first: 50
  ) {
    nodes {
      id identifier title description url updatedAt
      state { id name type }
      team { id key }
      labels { nodes { name } }
      relations(first: 50, includeArchived: true) {
        pageInfo { hasNextPage endCursor }
        nodes {
          type
          relatedIssue {
            id identifier archivedAt
            state { type }
          }
        }
      }
      inverseRelations(first: 50, includeArchived: true) {
        pageInfo { hasNextPage endCursor }
        nodes {
          type
          issue {
            id identifier archivedAt
            state { type }
          }
        }
      }
    }
  }
}
"""

ISSUE_RELATIONS_PAGE = """
query IssueRelationsPage($id: String!, $cursor: String) {
  issue(id: $id) {
    relations(first: 50, after: $cursor, includeArchived: true) {
      pageInfo { hasNextPage endCursor }
      nodes {
        type
        relatedIssue {
          id identifier archivedAt
          state { type }
        }
      }
    }
  }
}
"""

ISSUE_INVERSE_RELATIONS_PAGE = """
query IssueInverseRelationsPage($id: String!, $cursor: String) {
  issue(id: $id) {
    inverseRelations(first: 50, after: $cursor, includeArchived: true) {
      pageInfo { hasNextPage endCursor }
      nodes {
        type
        issue {
          id identifier archivedAt
          state { type }
        }
      }
    }
  }
}
"""

ISSUE_COMMENTS_SINCE = """
query IssueComments($id: String!, $after: DateTimeOrDuration!, $cursor: String) {
  issue(id: $id) {
    comments(
      first: 50,
      after: $cursor,
      filter: { createdAt: { gte: $after } },
      orderBy: createdAt
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id body createdAt
        user { id name email isMe }
        externalThread { type url }
      }
    }
  }
}
"""

CREATE_COMMENT = """
mutation CreateComment($input: CommentCreateInput!) {
  commentCreate(input: $input) {
    success
    comment { id createdAt }
  }
}
"""

UPDATE_ISSUE_STATE = """
mutation MoveIssue($id: String!, $stateId: String!) {
  issueUpdate(id: $id, input: { stateId: $stateId }) {
    success
  }
}
"""

TEAM_STATES = """
query TeamStates($key: String!) {
  team(id: $key) {
    id key
    states { nodes { id name type position } }
  }
}
"""

VIEWER_TEAMS = """
query Viewer {
  viewer {
    id name email
    teams(first: 50) { nodes { id key name } }
  }
}
"""
