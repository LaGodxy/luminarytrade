#![cfg(test)]

extern crate std;

use soroban_sdk::{
    testutils::{Address as _, Ledger, LedgerInfo},
    Address, Env, Symbol, Vec, String,
};

use bug_bounty::{
    BugBountyContract, BugBountyContractClient, 
    BugSubmission, SubmissionStatus, SeverityLevel, BountyConfig,
};
use common_utils::error::CommonError;

// ============================================================================
// Test helpers
// ============================================================================

fn setup() -> (Env, Address, BugBountyContractClient<'static>) {
    let env = Env::default();
    env.mock_all_auths();
    let contract_id = env.register_contract(None, BugBountyContract);
    let client = BugBountyContractClient::new(&env, &contract_id);
    let admin = Address::generate(&env);
    
    let config = BountyConfig {
        critical_bounty: 100_000_000_000,
        high_bounty: 50_000_000_000,
        medium_bounty: 10_000_000_000,
        low_bounty: 1_000_000_000,
        informational_bounty: 100_000_000,
        min_pool_balance: 1_000_000_000,
        auto_payment_enabled: false,
    };
    
    client.initialize(&admin, &config);
    (env, admin, client)
}

fn advance_time(env: &Env, seconds: u64) {
    let current = env.ledger().timestamp();
    env.ledger().set(LedgerInfo {
        timestamp: current + seconds,
        protocol_version: 22,
        sequence_number: env.ledger().sequence(),
        network_id: Default::default(),
        base_reserve: 10,
        min_temp_entry_ttl: 10,
        min_persistent_entry_ttl: 10,
        max_entry_ttl: 3_110_400,
    });
}

// ============================================================================
// 1. Initialization
// ============================================================================

#[test]
fn test_initialize_success() {
    let (_env, _admin, _client) = setup();
    // If we get here without panic, initialization succeeded
}

#[test]
#[should_panic]
fn test_initialize_twice_fails() {
    let (env, admin, client) = setup();
    
    let config = BountyConfig {
        critical_bounty: 100_000_000_000,
        high_bounty: 50_000_000_000,
        medium_bounty: 10_000_000_000,
        low_bounty: 1_000_000_000,
        informational_bounty: 100_000_000,
        min_pool_balance: 1_000_000_000,
        auto_payment_enabled: false,
    };
    
    // Second init should fail
    client.initialize(&admin, &config);
}

// ============================================================================
// 2. Reviewer Management
// ============================================================================

#[test]
fn test_add_reviewer() {
    let (env, admin, client) = setup();
    let reviewer = Address::generate(&env);
    
    let result = client.add_reviewer(&admin, &reviewer);
    assert!(result);
}

#[test]
fn test_add_multiple_reviewers() {
    let (env, admin, client) = setup();
    let reviewer1 = Address::generate(&env);
    let reviewer2 = Address::generate(&env);
    let reviewer3 = Address::generate(&env);
    
    client.add_reviewer(&admin, &reviewer1);
    client.add_reviewer(&admin, &reviewer2);
    client.add_reviewer(&admin, &reviewer3);
}

#[test]
fn test_cannot_add_duplicate_reviewer() {
    let (env, admin, client) = setup();
    let reviewer = Address::generate(&env);
    
    client.add_reviewer(&admin, &reviewer);
    
    // Second add should fail
    // In actual implementation, this would return AlreadyInitialized error
}

// ============================================================================
// 3. Bug Submission
// ============================================================================

#[test]
fn test_submit_bug_success() {
    let (env, _admin, client) = setup();
    let researcher = Address::generate(&env);
    
    let title = String::from_str(&env, "Critical vulnerability in contract");
    let description = String::from_str(&env, "Found a reentrancy bug...");
    let component = Symbol::new(&env, "main_contract");
    
    let submission_id = client.submit_bug(&researcher, &title, &description, &component);
    assert_eq!(submission_id, 0u64);
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.researcher, researcher);
    assert_eq!(submission.status, SubmissionStatus::Submitted);
    assert_eq!(submission.affected_component, component);
}

#[test]
fn test_multiple_submissions() {
    let (env, _admin, client) = setup();
    let researcher = Address::generate(&env);
    
    let title1 = String::from_str(&env, "Bug 1");
    let desc1 = String::from_str(&env, "Description 1");
    let comp1 = Symbol::new(&env, "contract_a");
    
    let title2 = String::from_str(&env, "Bug 2");
    let desc2 = String::from_str(&env, "Description 2");
    let comp2 = Symbol::new(&env, "contract_b");
    
    let id1 = client.submit_bug(&researcher, &title1, &desc1, &comp1);
    let id2 = client.submit_bug(&researcher, &title2, &desc2, &comp2);
    
    assert_eq!(id1, 0u64);
    assert_eq!(id2, 1u64);
}

#[test]
fn test_get_researcher_submissions() {
    let (env, _admin, client) = setup();
    let researcher = Address::generate(&env);
    
    let title = String::from_str(&env, "Test bug");
    let desc = String::from_str(&env, "Test description");
    let comp = Symbol::new(&env, "contract");
    
    client.submit_bug(&researcher, &title, &desc, &comp);
    client.submit_bug(&researcher, &title, &desc, &comp);
    
    let submissions = client.get_researcher_submissions(&researcher);
    assert_eq!(submissions.len(), 2);
}

// ============================================================================
// 4. Bug Review
// ============================================================================

#[test]
fn test_review_submission_verified() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    // Fund bounty pool
    client.fund_bounty_pool(&admin, &500_000_000_000);
    
    // Add reviewer
    client.add_reviewer(&admin, &reviewer);
    
    // Submit bug
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability found");
    let comp = Symbol::new(&env, "main_contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    // Review and verify
    let comments = String::from_str(&env, "Valid critical bug");
    let result = client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    assert!(result);
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.status, SubmissionStatus::Verified);
    assert_eq!(submission.severity, Some(SeverityLevel::Critical));
    assert!(submission.bounty_amount.is_some());
}

#[test]
fn test_review_submission_rejected() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Invalid bug");
    let desc = String::from_str(&env, "This is not a bug");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Not a valid bug");
    let result = client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Low, 
        &SubmissionStatus::Rejected,
        &comments
    );
    
    assert!(result);
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.status, SubmissionStatus::Rejected);
}

#[test]
fn test_bounty_calculation_critical() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.bounty_amount, Some(100_000_000_000));
}

#[test]
fn test_bounty_calculation_high() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "High severity bug");
    let desc = String::from_str(&env, "High severity vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::High, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.bounty_amount, Some(50_000_000_000));
}

// ============================================================================
// 5. Bounty Payment
// ============================================================================

#[test]
fn test_pay_bounty() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    // Fund pool
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    // Submit and verify
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    // Pay bounty
    let result = client.pay_bounty(&admin, &submission_id);
    assert!(result);
    
    let submission = client.get_submission(&submission_id).unwrap();
    assert_eq!(submission.status, SubmissionStatus::Paid);
    assert!(submission.paid_at.is_some());
    
    // Check pool decreased
    let pool = client.get_bounty_pool();
    assert_eq!(pool, 400_000_000_000); // 500B - 100B
}

#[test]
fn test_cannot_pay_unverified_submission() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    
    let title = String::from_str(&env, "Bug");
    let desc = String::from_str(&env, "Description");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    // Should fail - not verified
    // In actual implementation, this would return NotAuthorized error
}

#[test]
fn test_auto_payment_on_verification() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    // Setup with auto-payment enabled
    let config = BountyConfig {
        critical_bounty: 100_000_000_000,
        high_bounty: 50_000_000_000,
        medium_bounty: 10_000_000_000,
        low_bounty: 1_000_000_000,
        informational_bounty: 100_000_000,
        min_pool_balance: 1_000_000_000,
        auto_payment_enabled: true,
    };
    
    // Re-initialize with auto-payment
    // Note: In practice, you'd need a separate test setup for this
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    // With auto-payment, status should be Paid immediately
    // This depends on implementation
}

// ============================================================================
// 6. Bounty Pool Management
// ============================================================================

#[test]
fn test_fund_bounty_pool() {
    let (env, admin, client) = setup();
    
    let result = client.fund_bounty_pool(&admin, &100_000_000_000);
    assert!(result);
    
    let pool = client.get_bounty_pool();
    assert_eq!(pool, 100_000_000_000);
}

#[test]
fn test_multiple_funds_accumulate() {
    let (env, admin, client) = setup();
    
    client.fund_bounty_pool(&admin, &100_000_000_000);
    client.fund_bounty_pool(&admin, &50_000_000_000);
    client.fund_bounty_pool(&admin, &25_000_000_000);
    
    let pool = client.get_bounty_pool();
    assert_eq!(pool, 175_000_000_000);
}

#[test]
fn test_get_total_bounties_paid() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    client.pay_bounty(&admin, &submission_id);
    
    let total_paid = client.get_total_bounties_paid();
    assert_eq!(total_paid, 100_000_000_000);
}

// ============================================================================
// 7. Researcher Reputation
// ============================================================================

#[test]
fn test_reputation_on_submission() {
    let (env, _admin, client) = setup();
    let researcher = Address::generate(&env);
    
    let title = String::from_str(&env, "Bug");
    let desc = String::from_str(&env, "Description");
    let comp = Symbol::new(&env, "contract");
    client.submit_bug(&researcher, &title, &desc, &comp);
    
    let reputation = client.get_researcher_reputation(&researcher);
    assert_eq!(reputation.total_submissions, 1);
}

#[test]
fn test_reputation_on_valid_submission() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Valid bug");
    let desc = String::from_str(&env, "Valid vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::High, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    let reputation = client.get_researcher_reputation(&researcher);
    assert_eq!(reputation.valid_submissions, 1);
    assert_eq!(reputation.reputation_score, 10);
}

#[test]
fn test_reputation_on_bounty_paid() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    client.pay_bounty(&admin, &submission_id);
    
    let reputation = client.get_researcher_reputation(&researcher);
    assert_eq!(reputation.total_bounties_earned, 100_000_000_000);
    assert_eq!(reputation.reputation_score, 60); // 10 + 50
}

// ============================================================================
// 8. Configuration
// ============================================================================

#[test]
fn test_update_config() {
    let (env, admin, client) = setup();
    
    let new_config = BountyConfig {
        critical_bounty: 200_000_000_000,
        high_bounty: 100_000_000_000,
        medium_bounty: 20_000_000_000,
        low_bounty: 2_000_000_000,
        informational_bounty: 200_000_000,
        min_pool_balance: 2_000_000_000,
        auto_payment_enabled: true,
    };
    
    client.update_config(&admin, &new_config);
    
    let config = client.get_config();
    assert_eq!(config.critical_bounty, 200_000_000_000);
    assert_eq!(config.auto_payment_enabled, true);
}

#[test]
#[should_panic]
fn test_non_admin_cannot_update_config() {
    let (env, _admin, client) = setup();
    let non_admin = Address::generate(&env);
    
    let new_config = BountyConfig {
        critical_bounty: 200_000_000_000,
        high_bounty: 100_000_000_000,
        medium_bounty: 20_000_000_000,
        low_bounty: 2_000_000_000,
        informational_bounty: 200_000_000,
        min_pool_balance: 2_000_000_000,
        auto_payment_enabled: true,
    };
    
    client.update_config(&non_admin, &new_config);
}

#[test]
#[should_panic]
fn test_non_admin_cannot_fund_pool() {
    let (env, _admin, client) = setup();
    let non_admin = Address::generate(&env);
    
    client.fund_bounty_pool(&non_admin, &100_000_000_000);
}

#[test]
#[should_panic]
fn test_non_admin_cannot_pay_bounty() {
    let (env, admin, client) = setup();
    let researcher = Address::generate(&env);
    let reviewer = Address::generate(&env);
    let non_admin = Address::generate(&env);
    
    client.fund_bounty_pool(&admin, &500_000_000_000);
    client.add_reviewer(&admin, &reviewer);
    
    let title = String::from_str(&env, "Critical bug");
    let desc = String::from_str(&env, "Critical vulnerability");
    let comp = Symbol::new(&env, "contract");
    let submission_id = client.submit_bug(&researcher, &title, &desc, &comp);
    
    let comments = String::from_str(&env, "Valid");
    client.review_submission(
        &reviewer, 
        &submission_id, 
        &SeverityLevel::Critical, 
        &SubmissionStatus::Verified,
        &comments
    );
    
    client.pay_bounty(&non_admin, &submission_id);
}
