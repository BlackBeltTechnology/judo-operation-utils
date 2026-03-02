# Development Version and Branch Handling

## Branches

The versioning policy follows [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow).

| Branch Pattern | Purpose |
|---|---|
| `develop` | Main development branch — latest development sources of the current active version |
| `feature/JNG-NUMBER_short_summary` | Feature branches based on `develop` for new features |
| `(release/)X.Y.Z` | Release branches (the `release/` prefix is reserved for CI) |
| `bugfix/JNG-NUMBER_short_summary` | Bug fixes based on release branches — must be applied to newer release and develop branches too |
| `support/JNG-NUMBER_short_summary` | Support branches based on release branches for minor changes to previous releases |
| `master` | Latest released sources of the current active version |

```mermaid
gitGraph
    commit id: "init"
    branch develop
    checkout develop
    commit id: "dev-1"
    branch feature/JNG-1
    checkout feature/JNG-1
    commit id: "feat-1"
    commit id: "feat-2"
    checkout develop
    merge feature/JNG-1 id: "merge-feat-1"
    branch feature/JNG-3
    checkout feature/JNG-3
    commit id: "feat-3"
    checkout develop
    merge feature/JNG-3 id: "merge-feat-3"
    branch release/1.0-beta1
    checkout release/1.0-beta1
    commit id: "rc-1"
    branch bugfix/JNG-4
    checkout bugfix/JNG-4
    commit id: "fix-4"
    checkout release/1.0-beta1
    merge bugfix/JNG-4 id: "merge-fix"
    checkout master
    merge release/1.0-beta1 id: "release-1.0"
    checkout develop
    commit id: "dev-2"
    branch release/1.1-beta1
    checkout release/1.1-beta1
    commit id: "rc-2"
    checkout master
    merge release/1.1-beta1 id: "release-1.1"
```

## Version Numbers

Version numbers follow semantic versioning with these rules:

| Event | Version Change |
|---|---|
| Start a `feature/` branch | **No change** — keep the develop version |
| Start a `release/` branch from `develop` | Increment the **2nd number** on `develop` |
| Work on a `bugfix/` branch | **No change** — fixes are applied to the release branch before it merges to master |
| Start a `support/` branch | Increment the **3rd number** — used for minor updates to a previous release |
| Start a `hotfix/` branch | Increment the **4th number** — applied to both release and master branches |

## GitHub Action Flows

The CI/CD pipeline consists of four interconnected workflows:

```mermaid
flowchart TD
    subgraph Triggers
        PUSH[Push on develop]
        PR[PR on develop / master /<br/>increment / release]
        MANUAL[Manual trigger<br/>with version]
        MASTER_PUSH[Push on master]
        TAG[Push on merge-pr/* tag]
    end

    subgraph build.yml
        B_VERSION{Branch type?}
        B_BUILD[Build & deploy to Nexus]
        B_TAG[Create git tag v&lt;version&gt;]
        B_MERGE_TAG[Create merge-pr/&lt;version&gt; tag]
        B_RELEASE[Create GitHub pre-release<br/>with changelog]
    end

    subgraph merge-pr-tagged.yml
        M_CHECK{Version format?}
        M_MERGE[Merge PR to master]
        M_SQUASH[Squash PR to develop]
    end

    subgraph release.yml
        R_VERSION{Given version?}
        R_AUTO[Use version from pom.xml]
        R_CUSTOM[Use given version]
        R_NEXT[Calculate next version<br/>qualifier + 1]
        R_PR_MASTER[Create PR on master<br/>with release version]
        R_PR_DEVELOP[Create PR on develop<br/>with next version]
    end

    subgraph create-release-on-master.yml
        C_CHANGELOG[Build changelog]
        C_RELEASE[Create GitHub release]
    end

    PUSH --> B_VERSION
    PR --> B_VERSION
    B_VERSION -->|master, release/*| B_BUILD
    B_VERSION -->|develop, increment/*| B_BUILD
    B_BUILD --> B_TAG
    B_TAG -->|increment/*, release/*| B_MERGE_TAG
    B_TAG -->|develop| B_RELEASE
    B_MERGE_TAG --> TAG

    TAG --> M_CHECK
    M_CHECK -->|major.minor.qualifier| M_MERGE
    M_CHECK -->|other| M_SQUASH
    M_MERGE --> MASTER_PUSH
    M_SQUASH --> PUSH

    MANUAL --> R_VERSION
    R_VERSION -->|auto| R_AUTO
    R_VERSION -->|custom| R_CUSTOM
    R_AUTO --> R_NEXT
    R_CUSTOM --> R_NEXT
    R_NEXT --> R_PR_MASTER
    R_NEXT --> R_PR_DEVELOP
    R_PR_MASTER --> PR
    R_PR_DEVELOP --> PR

    MASTER_PUSH --> C_CHANGELOG
    C_CHANGELOG --> C_RELEASE
```

### build.yml

Triggered on pushes to `develop` and pull requests targeting `develop`, `master`, `increment/*`, or `release/*` branches.

1. **Version resolution:** For `master`/`release/*` branches, the version comes directly from `pom.xml` (without `-SNAPSHOT`). For `develop`/`increment/*`, a qualified version is generated: `major.minor.qualifier.date_commitId_branchName`.
2. **Build and deploy:** Compiles, tests, and deploys artifacts to Nexus.
3. **Tagging:** Creates a git tag `v<version>`. For `increment/*` and `release/*` branches, also creates a `merge-pr/<version>` tag that triggers the merge workflow.
4. **Pre-release:** For `develop` branch, generates a changelog and creates a GitHub pre-release.

### merge-pr-tagged.yml

Triggered when a `merge-pr/*` tag is pushed.

- If the version is in `major.minor.qualifier` format: **merges** the PR to `master` (triggers `create-release-on-master.yml`)
- Otherwise: **squashes** the PR to `develop` (triggers `build.yml`)
- Cleans up the `merge-pr/<version>` tag afterwards.

### create-release-on-master.yml

Triggered on pushes to `master`. Builds a changelog and creates a final GitHub release.

### release.yml

Manually triggered with a version parameter (`auto` or a specific `major.minor.qualifier`).

1. Resolves the release version (from `pom.xml` if `auto`)
2. Calculates the next development version (qualifier + 1)
3. Creates two PRs: one targeting `master` with the release version, one targeting `develop` with the next version

## How to Develop

Issue tracking uses [JIRA](https://blackbelt.atlassian.net/jira/dashboards).

> **Important:** There is no commit without a ticket number. Every pull request and commit must reference a `JNG-xxx` ticket.
