//! # Bug Bounty Contract
//!
//! A security bug bounty program with verified bug submissions and token-based rewards.
//!
//! ## Features
//!
//! - **Vulnerability Submission**: Secure process for reporting bugs
//! - **Verification Workflow**: Team review and validation of submissions
//! - **Token Bounties**: Automated bounty payments in tokens
//! - **Severity Classification**: Bugs classified by severity level
//! - **Transparent Tracking**: All submissions and payouts tracked on-chain
//!
//! ## Architecture
//!
//! ### Bug Bounty Flow
//!
//! 1. **Submission**: Researchers submit vulnerability reports
//! 2. **Initial Review**: Team validates the submission
//! 3. **Severity Assessment**: Bug is classified by severity
//! 4. **Verification**: Team verifies the bug is legitimate
//! 5. **Bounty Payment**: Automated payment based on severity
//! 6. **Resolution**: Bug is fixed and reporter is notified

#![no_std]

use soroban_sdk::{
    contract, contractimpl, contracttype, symbol_short, panic_with_error,
    Address, Env, Map, Symbol, Vec, String, IntoVal, Val
};
use common_utils::error::CommonError;

// ============================================================================
// Storage Keys
// ============================================================================

#[contracttype]
pub enum DataKey {
    // Configuration
    Admin,
    Initialized,
    BountyConfig,
    
    // Submission Management
    SubmissionCount,
    Submission(u64),
    UserSubmissions(Address),
    
    // Review & Verification
    ReviewTeam,
    ReviewerCount,
    
    // Bounty Payments
    BountyPool,
    TotalBountiesPaid,
    PaymentHistory(u64),
    PaymentCount,
    
    // Reputation
    ResearcherReputation(Address),
}

// ============================================================================
// Data Types
// ============================================================================

/// Bug severity level
#[derive(Clone, Copy, PartialEq, Eq)]
#[contracttype]
pub enum SeverityLevel {
    /// Critical - System compromise possible
    Critical = 0,
    /// High - Significant security impact
    High = 1,
    /// Medium - Moderate security impact
    Medium = 2,
    /// Low - Minor security issue
    Low = 3,
    /// Informational - Security best practice
    Informational = 4,
}

/// Submission status
#[derive(Clone, Copy, PartialEq, Eq)]
#[contracttype]
pub enum SubmissionStatus {
    /// Submitted, awaiting review
    Submitted = 0,
    /// Under review by team
    UnderReview = 1,
    /// Verified as valid bug
    Verified = 2,
    /// Invalid or duplicate
    Rejected = 3,
    /// Bounty paid
    Paid = 4,
    /// Duplicate submission
    Duplicate = 5,
}

/// Bug submission
#[derive(Clone)]
#[contracttype]
pub struct BugSubmission {
    /// Submission ID
    pub submission_id: u64,
    /// Researcher address
    pub researcher: Address,
    /// Bug title
    pub title: String,
    /// Bug description
    pub description: String,
    /// Affected contract/component
    pub affected_component: Symbol,
    /// Severity level (assessed by team)
    pub severity: Option<SeverityLevel>,
    /// Current status
    pub status: SubmissionStatus,
    /// Bounty amount (if verified)
    pub bounty_amount: Option<i128>,
    /// Submitted timestamp
    pub submitted_at: u64,
    /// Reviewed timestamp
    pub reviewed_at: Option<u64>,
    /// Paid timestamp
    pub paid_at: Option<u64>,
    /// Reviewer comments
    pub reviewer_comments: Option<String>,
}

/// Bounty configuration
#[derive(Clone)]
#[contracttype]
pub struct BountyConfig {
    /// Critical bug bounty amount
    pub critical_bounty: i128,
    /// High severity bounty amount
    pub high_bounty: i128,
    /// Medium severity bounty amount
    pub medium_bounty: i128,
    /// Low severity bounty amount
    pub low_bounty: i128,
    /// Informational bounty amount
    pub informational_bounty: i128,
    /// Minimum bounty pool balance
    pub min_pool_balance: i128,
    /// Enable auto-payment on verification
    pub auto_payment_enabled: bool,
}

/// Researcher reputation
#[derive(Clone)]
#[contracttype]
pub struct ResearcherReputation {
    /// Researcher address
    pub researcher: Address,
    /// Total submissions
    pub total_submissions: u32,
    /// Valid submissions
    pub valid_submissions: u32,
    /// Total bounties earned
    pub total_bounties_earned: i128,
    /// Reputation score
    pub reputation_score: u32,
    /// First submission timestamp
    pub first_submission_at: u64,
    /// Last submission timestamp
    pub last_submission_at: u64,
}

/// Payment record
#[derive(Clone)]
#[contracttype]
pub struct PaymentRecord {
    /// Payment ID
    pub payment_id: u64,
    /// Submission ID
    pub submission_id: u64,
    /// Researcher address
    pub researcher: Address,
    /// Amount paid
    pub amount: i128,
    /// Timestamp
    pub timestamp: u64,
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_CRITICAL_BOUNTY: i128 = 100_000_000_000; // 100K tokens
const DEFAULT_HIGH_BOUNTY: i128 = 50_000_000_000; // 50K tokens
const DEFAULT_MEDIUM_BOUNTY: i128 = 10_000_000_000; // 10K tokens
const DEFAULT_LOW_BOUNTY: i128 = 1_000_000_000; // 1K tokens
const DEFAULT_INFORMATIONAL_BOUNTY: i128 = 100_000_000; // 100 tokens

// ============================================================================
// Contract
// ============================================================================

#[contract]
pub struct BugBountyContract;

// ============================================================================
// Implementation
// ============================================================================

#[contractimpl]
impl BugBountyContract {
    /// Initialize the bug bounty contract
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `admin` - Admin address
    /// * `bounty_config` - Bounty configuration
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Initialization successful
    /// * `Err(CommonError)` - If already initialized
    pub fn initialize(
        env: Env,
        admin: Address,
        bounty_config: BountyConfig,
    ) -> Result<(), CommonError> {
        // Check if already initialized
        if env.storage().instance().has(&DataKey::Initialized) {
            return Err(CommonError::AlreadyInitialized);
        }

        // Store admin
        env.storage().instance().set(&DataKey::Admin, &admin);

        // Initialize configuration
        let config = BountyConfig {
            critical_bounty: if bounty_config.critical_bounty > 0 { 
                bounty_config.critical_bounty 
            } else { 
                DEFAULT_CRITICAL_BOUNTY 
            },
            high_bounty: if bounty_config.high_bounty > 0 { 
                bounty_config.high_bounty 
            } else { 
                DEFAULT_HIGH_BOUNTY 
            },
            medium_bounty: if bounty_config.medium_bounty > 0 { 
                bounty_config.medium_bounty 
            } else { 
                DEFAULT_MEDIUM_BOUNTY 
            },
            low_bounty: if bounty_config.low_bounty > 0 { 
                bounty_config.low_bounty 
            } else { 
                DEFAULT_LOW_BOUNTY 
            },
            informational_bounty: if bounty_config.informational_bounty > 0 { 
                bounty_config.informational_bounty 
            } else { 
                DEFAULT_INFORMATIONAL_BOUNTY 
            },
            min_pool_balance: bounty_config.min_pool_balance,
            auto_payment_enabled: bounty_config.auto_payment_enabled,
        };
        env.storage().instance().set(&DataKey::BountyConfig, &config);

        // Initialize counters
        env.storage().instance().set(&DataKey::SubmissionCount, &0u64);
        env.storage().instance().set(&DataKey::PaymentCount, &0u64);
        env.storage().instance().set(&DataKey::TotalBountiesPaid, &0i128);
        env.storage().instance().set(&DataKey::BountyPool, &0i128);
        env.storage().instance().set(&DataKey::ReviewerCount, &0u32);

        // Mark as initialized
        env.storage().instance().set(&DataKey::Initialized, &true);

        // Emit initialization event
        env.events().publish(
            (symbol_short!("bb_init"), admin),
            (config.critical_bounty, config.high_bounty),
        );

        Ok(())
    }

    /// Add review team member (admin only)
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `admin` - Admin address
    /// * `reviewer` - Reviewer address to add
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - True if added successfully
    /// * `Err(CommonError)` - If validation fails
    pub fn add_reviewer(
        env: Env,
        admin: Address,
        reviewer: Address,
    ) -> Result<bool, CommonError> {
        Self::require_admin(&env, &admin)?;

        // Check if already a reviewer
        let mut reviewers: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::ReviewTeam)
            .unwrap_or_else(|| Vec::new(&env));

        for existing_reviewer in reviewers.iter() {
            if existing_reviewer == reviewer {
                return Err(CommonError::AlreadyInitialized);
            }
        }

        // Add reviewer
        reviewers.push_back(reviewer.clone());
        env.storage().persistent().set(&DataKey::ReviewTeam, &reviewers);

        // Update count
        let mut count: u32 = env.storage().instance().get(&DataKey::ReviewerCount).unwrap_or(0);
        count += 1;
        env.storage().instance().set(&DataKey::ReviewerCount, &count);

        // Emit event
        env.events().publish(
            (symbol_short!("bb_add_rev"), admin),
            reviewer,
        );

        Ok(true)
    }

    /// Submit a bug report
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `researcher` - Researcher address
    /// * `title` - Bug title
    /// * `description` - Bug description
    /// * `affected_component` - Affected contract/component
    ///
    /// # Returns
    ///
    /// * `Ok(u64)` - Submission ID if successful
    /// * `Err(CommonError)` - If validation fails
    pub fn submit_bug(
        env: Env,
        researcher: Address,
        title: String,
        description: String,
        affected_component: Symbol,
    ) -> Result<u64, CommonError> {
        researcher.require_auth();

        // Check if already initialized
        if !env.storage().instance().has(&DataKey::Initialized) {
            return Err(CommonError::NotInitialized);
        }

        // Generate submission ID
        let submission_id: u64 = env.storage().instance().get(&DataKey::SubmissionCount).unwrap();

        // Create submission
        let submission = BugSubmission {
            submission_id,
            researcher: researcher.clone(),
            title,
            description,
            affected_component,
            severity: None,
            status: SubmissionStatus::Submitted,
            bounty_amount: None,
            submitted_at: env.ledger().timestamp(),
            reviewed_at: None,
            paid_at: None,
            reviewer_comments: None,
        };

        // Store submission
        env.storage().persistent().set(&DataKey::Submission(submission_id), &submission);
        env.storage().instance().set(&DataKey::SubmissionCount, &(submission_id + 1));

        // Update user submissions
        let mut user_subs: Vec<u64> = env
            .storage()
            .persistent()
            .get(&DataKey::UserSubmissions(researcher.clone()))
            .unwrap_or_else(|| Vec::new(&env));
        user_subs.push_back(submission_id);
        env.storage().persistent().set(&DataKey::UserSubmissions(researcher.clone()), &user_subs);

        // Update researcher reputation
        Self::update_reputation_on_submission(&env, researcher.clone(), submission_id)?;

        // Emit event
        env.events().publish(
            (symbol_short!("bb_submit"), researcher),
            (submission_id, affected_component),
        );

        Ok(submission_id)
    }

    /// Review and assess a bug submission (reviewer only)
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `reviewer` - Reviewer address
    /// * `submission_id` - Submission ID to review
    /// * `severity` - Assessed severity level
    /// * `status` - New status (Verified, Rejected, Duplicate)
    /// * `comments` - Reviewer comments
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - True if review successful
    /// * `Err(CommonError)` - If validation fails
    pub fn review_submission(
        env: Env,
        reviewer: Address,
        submission_id: u64,
        severity: SeverityLevel,
        status: SubmissionStatus,
        comments: String,
    ) -> Result<bool, CommonError> {
        // Verify reviewer
        Self::require_reviewer(&env, &reviewer)?;

        // Get submission
        let mut submission: BugSubmission = env
            .storage()
            .persistent()
            .get(&DataKey::Submission(submission_id))
            .ok_or(CommonError::KeyNotFound)?;

        // Check submission is under review or submitted
        if submission.status != SubmissionStatus::Submitted && 
           submission.status != SubmissionStatus::UnderReview {
            return Err(CommonError::NotAuthorized);
        }

        // Update submission
        submission.severity = Some(severity);
        submission.status = status.clone();
        submission.reviewed_at = Some(env.ledger().timestamp());
        submission.reviewer_comments = Some(comments);

        // Calculate bounty amount if verified
        if status == SubmissionStatus::Verified {
            let bounty_amount = Self::calculate_bounty(&env, severity)?;
            submission.bounty_amount = Some(bounty_amount);

            // Auto-payment if enabled
            let config: BountyConfig = env.storage().instance().get(&DataKey::BountyConfig).unwrap();
            if config.auto_payment_enabled {
                Self::pay_bounty_internal(&env, submission_id, &submission)?;
            }
        }

        // Store updated submission
        env.storage().persistent().set(&DataKey::Submission(submission_id), &submission);

        // Update researcher reputation
        if status == SubmissionStatus::Verified || status == SubmissionStatus::Paid {
            Self::update_reputation_on_valid_submission(&env, submission.researcher.clone(), submission_id)?;
        }

        // Emit event
        env.events().publish(
            (symbol_short!("bb_review"), reviewer),
            (submission_id, status as u32, severity as u32),
        );

        Ok(true)
    }

    /// Pay bounty for a verified submission (admin only)
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `admin` - Admin address
    /// * `submission_id` - Submission ID to pay
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - True if payment successful
    /// * `Err(CommonError)` - If validation fails
    pub fn pay_bounty(
        env: Env,
        admin: Address,
        submission_id: u64,
    ) -> Result<bool, CommonError> {
        Self::require_admin(&env, &admin)?;

        // Get submission
        let submission: BugSubmission = env
            .storage()
            .persistent()
            .get(&DataKey::Submission(submission_id))
            .ok_or(CommonError::KeyNotFound)?;

        // Check submission is verified
        if submission.status != SubmissionStatus::Verified {
            return Err(CommonError::NotAuthorized);
        }

        Self::pay_bounty_internal(&env, submission_id, &submission)
    }

    /// Fund the bounty pool (admin only)
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `admin` - Admin address
    /// * `amount` - Amount to add to pool
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` - True if funding successful
    /// * `Err(CommonError)` - If validation fails
    pub fn fund_bounty_pool(
        env: Env,
        admin: Address,
        amount: i128,
    ) -> Result<bool, CommonError> {
        Self::require_admin(&env, &admin)?;

        if amount <= 0 {
            return Err(CommonError::OutOfRange);
        }

        // Update bounty pool
        let mut pool: i128 = env.storage().instance().get(&DataKey::BountyPool).unwrap_or(0);
        pool += amount;
        env.storage().instance().set(&DataKey::BountyPool, &pool);

        // Emit event
        env.events().publish(
            (symbol_short!("bb_fund"), admin),
            (amount, pool),
        );

        Ok(true)
    }

    /// Get bug submission details
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `submission_id` - Submission ID
    ///
    /// # Returns
    ///
    /// * `Option<BugSubmission>` - Submission details if exists
    pub fn get_submission(env: Env, submission_id: u64) -> Option<BugSubmission> {
        env.storage().persistent().get(&DataKey::Submission(submission_id))
    }

    /// Get researcher's submissions
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `researcher` - Researcher address
    ///
    /// # Returns
    ///
    /// * `Vec<u64>` - List of submission IDs
    pub fn get_researcher_submissions(env: Env, researcher: Address) -> Vec<u64> {
        env.storage()
            .persistent()
            .get(&DataKey::UserSubmissions(researcher))
            .unwrap_or_else(|| Vec::new(&env))
    }

    /// Get researcher reputation
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `researcher` - Researcher address
    ///
    /// # Returns
    ///
    /// * `ResearcherReputation` - Researcher's reputation
    pub fn get_researcher_reputation(env: Env, researcher: Address) -> ResearcherReputation {
        env.storage()
            .persistent()
            .get(&DataKey::ResearcherReputation(researcher))
            .unwrap_or_else(|| ResearcherReputation {
                researcher: Address::generate(&env),
                total_submissions: 0,
                valid_submissions: 0,
                total_bounties_earned: 0,
                reputation_score: 0,
                first_submission_at: 0,
                last_submission_at: 0,
            })
    }

    /// Get bounty pool balance
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    ///
    /// # Returns
    ///
    /// * `i128` - Current bounty pool balance
    pub fn get_bounty_pool(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::BountyPool).unwrap_or(0)
    }

    /// Get total bounties paid
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    ///
    /// # Returns
    ///
    /// * `i128` - Total bounties paid
    pub fn get_total_bounties_paid(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::TotalBountiesPaid).unwrap_or(0)
    }

    /// Get bounty configuration
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    ///
    /// # Returns
    ///
    /// * `BountyConfig` - Current configuration
    pub fn get_config(env: Env) -> BountyConfig {
        env.storage().instance().get(&DataKey::BountyConfig).unwrap()
    }

    /// Update bounty configuration (admin only)
    ///
    /// # Arguments
    ///
    /// * `env` - Soroban environment
    /// * `admin` - Admin address
    /// * `config` - New configuration
    pub fn update_config(
        env: Env,
        admin: Address,
        config: BountyConfig,
    ) -> Result<(), CommonError> {
        Self::require_admin(&env, &admin)?;
        env.storage().instance().set(&DataKey::BountyConfig, &config);
        
        env.events().publish(
            (symbol_short!("bb_cfg_upd"), admin),
            symbol_short!("updated"),
        );

        Ok(())
    }

    // ========================================================================
    // Internal Helper Functions
    // ========================================================================

    /// Calculate bounty amount based on severity
    fn calculate_bounty(env: &Env, severity: SeverityLevel) -> Result<i128, CommonError> {
        let config: BountyConfig = env.storage().instance().get(&DataKey::BountyConfig).unwrap();

        let bounty = match severity {
            SeverityLevel::Critical => config.critical_bounty,
            SeverityLevel::High => config.high_bounty,
            SeverityLevel::Medium => config.medium_bounty,
            SeverityLevel::Low => config.low_bounty,
            SeverityLevel::Informational => config.informational_bounty,
        };

        Ok(bounty)
    }

    /// Internal function to pay bounty
    fn pay_bounty_internal(
        env: &Env,
        submission_id: u64,
        submission: &BugSubmission,
    ) -> Result<bool, CommonError> {
        // Get bounty amount
        let bounty_amount = submission.bounty_amount.ok_or(CommonError::KeyNotFound)?;

        // Check bounty pool has sufficient funds
        let mut pool: i128 = env.storage().instance().get(&DataKey::BountyPool).unwrap_or(0);
        if pool < bounty_amount {
            return Err(CommonError::OutOfRange);
        }

        // Deduct from pool
        pool -= bounty_amount;
        env.storage().instance().set(&DataKey::BountyPool, &pool);

        // Update submission status
        let mut updated_submission = submission.clone();
        updated_submission.status = SubmissionStatus::Paid;
        updated_submission.paid_at = Some(env.ledger().timestamp());
        env.storage().persistent().set(&DataKey::Submission(submission_id), &updated_submission);

        // Record payment
        let payment_count: u64 = env.storage().instance().get(&DataKey::PaymentCount).unwrap_or(0);
        let payment = PaymentRecord {
            payment_id: payment_count,
            submission_id,
            researcher: submission.researcher.clone(),
            amount: bounty_amount,
            timestamp: env.ledger().timestamp(),
        };
        env.storage().persistent().set(&DataKey::PaymentHistory(payment_count), &payment);
        env.storage().instance().set(&DataKey::PaymentCount, &(payment_count + 1));

        // Update total bounties paid
        let mut total_paid: i128 = env.storage().instance().get(&DataKey::TotalBountiesPaid).unwrap_or(0);
        total_paid += bounty_amount;
        env.storage().instance().set(&DataKey::TotalBountiesPaid, &total_paid);

        // Update researcher reputation
        Self::update_reputation_on_bounty_paid(env, submission.researcher.clone(), bounty_amount)?;

        // Emit event
        env.events().publish(
            (symbol_short!("bb_pay"), submission.researcher.clone()),
            (submission_id, bounty_amount),
        );

        Ok(true)
    }

    /// Update researcher reputation on submission
    fn update_reputation_on_submission(
        env: &Env,
        researcher: Address,
        submission_id: u64,
    ) -> Result<(), CommonError> {
        let mut reputation: ResearcherReputation = env
            .storage()
            .persistent()
            .get(&DataKey::ResearcherReputation(researcher.clone()))
            .unwrap_or_else(|| ResearcherReputation {
                researcher: researcher.clone(),
                total_submissions: 0,
                valid_submissions: 0,
                total_bounties_earned: 0,
                reputation_score: 0,
                first_submission_at: env.ledger().timestamp(),
                last_submission_at: env.ledger().timestamp(),
            });

        reputation.total_submissions += 1;
        reputation.last_submission_at = env.ledger().timestamp();

        env.storage().persistent().set(&DataKey::ResearcherReputation(researcher), &reputation);

        Ok(())
    }

    /// Update researcher reputation on valid submission
    fn update_reputation_on_valid_submission(
        env: &Env,
        researcher: Address,
        submission_id: u64,
    ) -> Result<(), CommonError> {
        if let Some(mut reputation) = env.storage().persistent().get::<_, ResearcherReputation>(
            &DataKey::ResearcherReputation(researcher.clone())
        ) {
            reputation.valid_submissions += 1;
            reputation.reputation_score += 10; // Add 10 points for valid submission
            env.storage().persistent().set(&DataKey::ResearcherReputation(researcher), &reputation);
        }

        Ok(())
    }

    /// Update researcher reputation on bounty paid
    fn update_reputation_on_bounty_paid(
        env: &Env,
        researcher: Address,
        bounty_amount: i128,
    ) -> Result<(), CommonError> {
        if let Some(mut reputation) = env.storage().persistent().get::<_, ResearcherReputation>(
            &DataKey::ResearcherReputation(researcher.clone())
        ) {
            reputation.total_bounties_earned += bounty_amount;
            reputation.reputation_score += 50; // Add 50 points for bounty paid
            env.storage().persistent().set(&DataKey::ResearcherReputation(researcher), &reputation);
        }

        Ok(())
    }

    /// Require admin authorization
    fn require_admin(env: &Env, admin: &Address) -> Result<(), CommonError> {
        let stored_admin: Address = env
            .storage()
            .instance()
            .get(&DataKey::Admin)
            .ok_or(CommonError::NotInitialized)?;

        if stored_admin != *admin {
            return Err(CommonError::NotAuthorized);
        }

        admin.require_auth();
        Ok(())
    }

    /// Require reviewer authorization
    fn require_reviewer(env: &Env, reviewer: &Address) -> Result<(), CommonError> {
        let reviewers: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::ReviewTeam)
            .unwrap_or_else(|| Vec::new(env));

        for existing_reviewer in reviewers.iter() {
            if existing_reviewer == *reviewer {
                reviewer.require_auth();
                return Ok(());
            }
        }

        Err(CommonError::NotAuthorized)
    }
}
