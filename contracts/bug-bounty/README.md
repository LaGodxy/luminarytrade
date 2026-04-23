# Bug Bounty Contract

A security bug bounty program with verified bug submissions and token-based rewards on Stellar/Soroban.

## Overview

This contract implements a comprehensive bug bounty program where security researchers can submit vulnerability reports, have them reviewed by the team, and receive token-based bounties for verified bugs. The system ensures transparency, fair compensation, and proper tracking of all submissions.

## Features

- **Vulnerability Submission**: Secure process for reporting bugs
- **Verification Workflow**: Team review and validation of submissions
- **Token Bounties**: Automated bounty payments in tokens
- **Severity Classification**: Bugs classified by severity level (Critical, High, Medium, Low, Informational)
- **Transparent Tracking**: All submissions and payouts tracked on-chain
- **Researcher Reputation**: Track researcher contributions and reputation
- **Configurable Rewards**: Flexible bounty amounts per severity level
- **Auto-Payment Option**: Automatic bounty payment on verification

## Contract Functions

### Initialization
- `initialize(admin, bounty_config)` - Initialize the contract

### Team Management
- `add_reviewer(admin, reviewer)` - Add review team member (admin only)

### Bug Submission
- `submit_bug(researcher, title, description, affected_component)` - Submit bug report

### Review & Verification
- `review_submission(reviewer, submission_id, severity, status, comments)` - Review submission

### Bounty Payments
- `pay_bounty(admin, submission_id)` - Pay bounty for verified submission (admin only)
- `fund_bounty_pool(admin, amount)` - Fund the bounty pool (admin only)

### Query Functions
- `get_submission(submission_id)` - Get submission details
- `get_researcher_submissions(researcher)` - Get researcher's submissions
- `get_researcher_reputation(researcher)` - Get researcher reputation
- `get_bounty_pool()` - Get bounty pool balance
- `get_total_bounties_paid()` - Get total bounties paid
- `get_config()` - Get bounty configuration

### Configuration
- `update_config(admin, config)` - Update bounty configuration (admin only)

## Data Structures

### BugSubmission
```rust
pub struct BugSubmission {
    pub submission_id: u64,
    pub researcher: Address,
    pub title: String,
    pub description: String,
    pub affected_component: Symbol,
    pub severity: Option<SeverityLevel>,
    pub status: SubmissionStatus,
    pub bounty_amount: Option<i128>,
    pub submitted_at: u64,
    pub reviewed_at: Option<u64>,
    pub paid_at: Option<u64>,
    pub reviewer_comments: Option<String>,
}
```

### SeverityLevel
- `Critical` - System compromise possible (highest bounty)
- `High` - Significant security impact
- `Medium` - Moderate security impact
- `Low` - Minor security issue
- `Informational` - Security best practice (lowest bounty)

### SubmissionStatus
- `Submitted` - Submitted, awaiting review
- `UnderReview` - Under review by team
- `Verified` - Verified as valid bug
- `Rejected` - Invalid or duplicate
- `Paid` - Bounty paid
- `Duplicate` - Duplicate submission

### ResearcherReputation
```rust
pub struct ResearcherReputation {
    pub researcher: Address,
    pub total_submissions: u32,
    pub valid_submissions: u32,
    pub total_bounties_earned: i128,
    pub reputation_score: u32,
    pub first_submission_at: u64,
    pub last_submission_at: u64,
}
```

### BountyConfig
```rust
pub struct BountyConfig {
    pub critical_bounty: i128,
    pub high_bounty: i128,
    pub medium_bounty: i128,
    pub low_bounty: i128,
    pub informational_bounty: i128,
    pub min_pool_balance: i128,
    pub auto_payment_enabled: bool,
}
```

## Usage Example

```rust
// Initialize contract
let config = BountyConfig {
    critical_bounty: 100_000_000_000,    // 100K tokens
    high_bounty: 50_000_000_000,         // 50K tokens
    medium_bounty: 10_000_000_000,       // 10K tokens
    low_bounty: 1_000_000_000,           // 1K tokens
    informational_bounty: 100_000_000,   // 100 tokens
    min_pool_balance: 1_000_000_000,
    auto_payment_enabled: false,
};
client.initialize(&admin, &config);

// Fund bounty pool
client.fund_bounty_pool(&admin, &500_000_000_000);

// Add reviewer
client.add_reviewer(&admin, &reviewer);

// Researcher submits bug
let submission_id = client.submit_bug(
    &researcher,
    &String::from_str(&env, "Critical reentrancy bug"),
    &String::from_str(&env, "Found vulnerability in..."),
    &Symbol::new(&env, "main_contract")
);

// Reviewer verifies bug
client.review_submission(
    &reviewer,
    &submission_id,
    &SeverityLevel::Critical,
    &SubmissionStatus::Verified,
    &String::from_str(&env, "Valid critical bug")
);

// Admin pays bounty
client.pay_bounty(&admin, &submission_id);
```

## Bounty Structure

Default bounty amounts (configurable):
- **Critical**: 100,000 tokens
- **High**: 50,000 tokens
- **Medium**: 10,000 tokens
- **Low**: 1,000 tokens
- **Informational**: 100 tokens

## Reputation System

Researchers earn reputation points:
- **Submission**: Initial registration in system
- **Valid Submission**: +10 points
- **Bounty Paid**: +50 points

Higher reputation indicates trusted and prolific security researchers.

## Events

- `bb_init` - Contract initialized
- `bb_add_rev` - Reviewer added
- `bb_submit` - Bug submitted
- `bb_review` - Bug reviewed
- `bb_pay` - Bounty paid
- `bb_fund` - Bounty pool funded
- `bb_cfg_upd` - Configuration updated

## Security Considerations

- Authentication required for all state-changing operations
- Only authorized reviewers can assess submissions
- Admin controls for payments and configuration
- Bounty pool must be funded before payments
- All transactions are transparent and auditable
- Duplicate detection prevents double payments

## Testing

Run tests with:
```bash
cargo test
```

## Deployment

Build the contract:
```bash
soroban contract build
```

Deploy to testnet:
```bash
soroban contract deploy --wasm target/wasm32-unknown-unknown/release/bug_bounty.wasm
```
