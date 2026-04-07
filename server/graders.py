"""Graders for evaluating agent performance on email triage tasks."""

from typing import Any, Dict, List

from models import (
    Email, EmailCategory, EmailPriority, Reward
)


def calculate_categorization_accuracy(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Calculate accuracy of email categorization with partial credit."""
    if not emails:
        return 0.0
    
    total_score = 0.0
    
    # Category similarity groups for partial credit
    similar_categories = {
        EmailCategory.CUSTOMER_SUPPORT: [EmailCategory.BILLING, EmailCategory.TECHNICAL],
        EmailCategory.BILLING: [EmailCategory.CUSTOMER_SUPPORT],
        EmailCategory.TECHNICAL: [EmailCategory.CUSTOMER_SUPPORT],
        EmailCategory.SALES: [EmailCategory.CUSTOMER_SUPPORT],
        EmailCategory.INTERNAL: [EmailCategory.NEWSLETTER],
        EmailCategory.NEWSLETTER: [EmailCategory.INTERNAL],
        EmailCategory.SPAM: [],  # No partial credit for spam misclassification
    }
    
    for email in emails:
        expected = ground_truth.get(email.id, {}).get("correct_category")
        
        if email.category is None:
            # Penalize uncategorized emails
            total_score += 0.0
        elif email.category == expected:
            # Full credit for correct
            total_score += 1.0
        elif expected in similar_categories and email.category in similar_categories.get(expected, []):
            # Partial credit for similar categories
            total_score += 0.3
        else:
            # No credit for wrong category
            total_score += 0.0
    
    return total_score / len(emails)


def calculate_prioritization_accuracy(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Calculate accuracy of email prioritization with partial credit."""
    if not emails:
        return 0.0
    
    total_score = 0.0
    priority_levels = {
        EmailPriority.URGENT: 4,
        EmailPriority.HIGH: 3,
        EmailPriority.NORMAL: 2,
        EmailPriority.LOW: 1
    }
    
    for email in emails:
        expected = ground_truth.get(email.id, {}).get("correct_priority")
        
        if email.priority is None:
            total_score += 0.0
        elif email.priority == expected:
            total_score += 1.0
        else:
            # Partial credit based on how close the priority is
            expected_level = priority_levels.get(expected, 2)
            actual_level = priority_levels.get(email.priority, 2)
            diff = abs(expected_level - actual_level)
            
            if diff == 1:
                total_score += 0.5  # Off by one level
            elif diff == 2:
                total_score += 0.2  # Off by two levels
            else:
                total_score += 0.0  # Completely wrong
    
    return total_score / len(emails)
    
    if prioritized == 0:
        return 0.0
    
    return correct / len(emails)


def calculate_spam_detection_accuracy(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Calculate spam detection accuracy."""
    spam_emails = [
        e for e in emails 
        if ground_truth.get(e.id, {}).get("is_spam", False)
    ]
    
    if not spam_emails:
        return 1.0  # No spam to detect
    
    correctly_marked = sum(
        1 for e in spam_emails 
        if e.is_spam or e.category == EmailCategory.SPAM
    )
    
    return correctly_marked / len(spam_emails)


def check_urgent_flagged(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Check if urgent emails are flagged."""
    urgent_emails = [
        e for e in emails
        if ground_truth.get(e.id, {}).get("correct_priority") == EmailPriority.URGENT
    ]
    
    if not urgent_emails:
        return 1.0
    
    flagged = sum(1 for e in urgent_emails if e.is_flagged)
    return flagged / len(urgent_emails)


def check_newsletters_archived(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Check if newsletters are archived."""
    newsletters = [
        e for e in emails
        if ground_truth.get(e.id, {}).get("correct_category") == EmailCategory.NEWSLETTER
    ]
    
    if not newsletters:
        return 1.0
    
    archived = sum(1 for e in newsletters if e.is_archived)
    return archived / len(newsletters)


def check_customer_support_replied(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Check if customer support emails received replies."""
    support_emails = [
        e for e in emails
        if (ground_truth.get(e.id, {}).get("correct_category") == EmailCategory.CUSTOMER_SUPPORT
            and ground_truth.get(e.id, {}).get("requires_reply", False))
    ]
    
    if not support_emails:
        return 1.0
    
    replied = sum(1 for e in support_emails if e.reply_sent)
    return replied / len(support_emails)


def check_technical_forwarded(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]]
) -> float:
    """Check if technical issues are forwarded."""
    tech_emails = [
        e for e in emails
        if ground_truth.get(e.id, {}).get("correct_category") == EmailCategory.TECHNICAL
    ]
    
    if not tech_emails:
        return 1.0
    
    forwarded = sum(1 for e in tech_emails if e.forwarded_to is not None)
    return forwarded / len(tech_emails)


def calculate_efficiency_score(
    step_count: int,
    max_steps: int,
    emails_processed: int,
    total_emails: int
) -> float:
    """Calculate efficiency bonus based on steps used."""
    if total_emails == 0:
        return 0.0
    
    completion_ratio = emails_processed / total_emails
    step_efficiency = 1.0 - (step_count / max_steps)
    
    # Bonus for completing efficiently
    return completion_ratio * max(0, step_efficiency) * 0.5


def grade_task_easy(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]],
    step_count: int,
    max_steps: int
) -> Dict[str, Any]:
    """Grade the easy categorization task."""
    cat_accuracy = calculate_categorization_accuracy(emails, ground_truth)
    spam_accuracy = calculate_spam_detection_accuracy(emails, ground_truth)
    
    # Check if all emails have been processed (categorized)
    all_categorized = all(e.category is not None for e in emails)
    
    # Calculate final score
    score = (
        cat_accuracy * 0.5 +
        spam_accuracy * 0.3 +
        (0.2 if all_categorized else 0.0)
    )
    
    return {
        "score": min(1.0, max(0.0, score)),
        "breakdown": {
            "categorization_accuracy": cat_accuracy,
            "spam_detection_accuracy": spam_accuracy,
            "all_categorized": 1.0 if all_categorized else 0.0
        },
        "passed": score >= 0.6
    }


def grade_task_medium(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]],
    step_count: int,
    max_steps: int
) -> Dict[str, Any]:
    """Grade the medium triage task."""
    cat_accuracy = calculate_categorization_accuracy(emails, ground_truth)
    pri_accuracy = calculate_prioritization_accuracy(emails, ground_truth)
    spam_accuracy = calculate_spam_detection_accuracy(emails, ground_truth)
    support_replied = check_customer_support_replied(emails, ground_truth)
    
    # Flagging high priority
    high_pri_flagged = check_urgent_flagged(emails, ground_truth)
    
    # Calculate final score
    score = (
        cat_accuracy * 0.25 +
        pri_accuracy * 0.2 +
        spam_accuracy * 0.2 +
        high_pri_flagged * 0.15 +
        support_replied * 0.2
    )
    
    return {
        "score": min(1.0, max(0.0, score)),
        "breakdown": {
            "categorization_accuracy": cat_accuracy,
            "prioritization_accuracy": pri_accuracy,
            "spam_detection_accuracy": spam_accuracy,
            "high_priority_flagged": high_pri_flagged,
            "customer_support_replied": support_replied
        },
        "passed": score >= 0.6
    }


def grade_task_hard(
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]],
    step_count: int,
    max_steps: int
) -> Dict[str, Any]:
    """Grade the hard full inbox management task."""
    cat_accuracy = calculate_categorization_accuracy(emails, ground_truth)
    pri_accuracy = calculate_prioritization_accuracy(emails, ground_truth)
    spam_accuracy = calculate_spam_detection_accuracy(emails, ground_truth)
    urgent_flagged = check_urgent_flagged(emails, ground_truth)
    newsletters_archived = check_newsletters_archived(emails, ground_truth)
    support_replied = check_customer_support_replied(emails, ground_truth)
    tech_forwarded = check_technical_forwarded(emails, ground_truth)
    
    # Count processed emails
    processed = sum(1 for e in emails if e.category is not None)
    efficiency = calculate_efficiency_score(step_count, max_steps, processed, len(emails))
    
    # Calculate final score
    score = (
        cat_accuracy * 0.2 +
        pri_accuracy * 0.15 +
        spam_accuracy * 0.15 +
        urgent_flagged * 0.1 +
        newsletters_archived * 0.1 +
        support_replied * 0.1 +
        tech_forwarded * 0.1 +
        efficiency * 0.1
    )
    
    return {
        "score": min(1.0, max(0.0, score)),
        "breakdown": {
            "categorization_accuracy": cat_accuracy,
            "prioritization_accuracy": pri_accuracy,
            "spam_detection_accuracy": spam_accuracy,
            "urgent_items_flagged": urgent_flagged,
            "newsletters_archived": newsletters_archived,
            "customer_support_replied": support_replied,
            "technical_forwarded": tech_forwarded,
            "efficiency_bonus": efficiency
        },
        "passed": score >= 0.6
    }


def grade_task(
    task_id: str,
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]],
    step_count: int,
    max_steps: int
) -> Dict[str, Any]:
    """Grade a task based on task_id."""
    if task_id == "task_easy_categorize":
        return grade_task_easy(emails, ground_truth, step_count, max_steps)
    elif task_id == "task_medium_triage":
        return grade_task_medium(emails, ground_truth, step_count, max_steps)
    elif task_id == "task_hard_full_inbox":
        return grade_task_hard(emails, ground_truth, step_count, max_steps)
    else:
        raise ValueError(f"Unknown task: {task_id}")


def calculate_step_reward(
    action_type: str,
    action_result: Dict[str, Any],
    emails: List[Email],
    ground_truth: Dict[str, Dict[str, Any]],
    previous_state: Dict[str, Any]
) -> Reward:
    """Calculate reward for a single step."""
    reward_value = 0.0
    breakdown = {}
    messages = []
    
    email_id = action_result.get("email_id")
    success = action_result.get("success", False)
    
    if not success:
        # Penalize invalid actions
        reward_value = -0.1
        messages.append("Invalid action")
        breakdown["invalid_action"] = -0.1
    else:
        if action_type == "categorize":
            # Check if categorization is correct
            email = next((e for e in emails if e.id == email_id), None)
            if email and email.category:
                expected = ground_truth.get(email_id, {}).get("correct_category")
                if email.category == expected:
                    reward_value += 0.1
                    messages.append("Correct categorization")
                    breakdown["correct_category"] = 0.1
                else:
                    reward_value += 0.02  # Small reward for attempting
                    messages.append("Categorized (incorrect)")
                    breakdown["categorize_attempt"] = 0.02
        
        elif action_type == "prioritize":
            email = next((e for e in emails if e.id == email_id), None)
            if email and email.priority:
                expected = ground_truth.get(email_id, {}).get("correct_priority")
                if email.priority == expected:
                    reward_value += 0.08
                    messages.append("Correct priority")
                    breakdown["correct_priority"] = 0.08
                else:
                    reward_value += 0.02
                    messages.append("Priority set (incorrect)")
                    breakdown["prioritize_attempt"] = 0.02
        
        elif action_type == "mark_spam":
            email = next((e for e in emails if e.id == email_id), None)
            if email:
                is_spam = ground_truth.get(email_id, {}).get("is_spam", False)
                if is_spam:
                    reward_value += 0.15
                    messages.append("Correctly marked spam")
                    breakdown["correct_spam"] = 0.15
                else:
                    reward_value -= 0.1
                    messages.append("Incorrectly marked as spam")
                    breakdown["false_spam"] = -0.1
        
        elif action_type == "reply":
            email = next((e for e in emails if e.id == email_id), None)
            if email:
                requires_reply = ground_truth.get(email_id, {}).get("requires_reply", False)
                if requires_reply:
                    reward_value += 0.12
                    messages.append("Replied to email requiring response")
                    breakdown["needed_reply"] = 0.12
                else:
                    reward_value += 0.02
                    messages.append("Reply sent (not required)")
                    breakdown["unnecessary_reply"] = 0.02
        
        elif action_type == "forward":
            email = next((e for e in emails if e.id == email_id), None)
            if email:
                category = ground_truth.get(email_id, {}).get("correct_category")
                if category == "technical":
                    reward_value += 0.1
                    messages.append("Correctly forwarded technical issue")
                    breakdown["correct_forward"] = 0.1
                else:
                    reward_value += 0.01
                    messages.append("Forwarded (not technical)")
                    breakdown["forward"] = 0.01
        
        elif action_type == "archive":
            email = next((e for e in emails if e.id == email_id), None)
            if email:
                should_archive = ground_truth.get(email_id, {}).get("should_archive", False)
                if should_archive:
                    reward_value += 0.08
                    messages.append("Correctly archived")
                    breakdown["correct_archive"] = 0.08
                else:
                    reward_value += 0.01
                    messages.append("Archived")
                    breakdown["archive"] = 0.01
        
        elif action_type == "flag":
            email = next((e for e in emails if e.id == email_id), None)
            if email:
                priority = ground_truth.get(email_id, {}).get("correct_priority")
                if priority in [EmailPriority.URGENT, EmailPriority.HIGH]:
                    reward_value += 0.1
                    messages.append("Flagged high-priority email")
                    breakdown["correct_flag"] = 0.1
                else:
                    reward_value += 0.01
                    messages.append("Flagged (low priority)")
                    breakdown["flag"] = 0.01
        
        elif action_type == "done":
            # Calculate completion bonus based on progress
            processed = sum(1 for e in emails if e.category is not None)
            completion_ratio = processed / len(emails) if emails else 0
            if completion_ratio >= 0.9:
                reward_value += 0.2
                messages.append("Task completed with high coverage")
                breakdown["completion_bonus"] = 0.2
            elif completion_ratio >= 0.5:
                reward_value += 0.1
                messages.append("Task completed with partial coverage")
                breakdown["partial_completion"] = 0.1
    
    return Reward(
        value=max(-1.0, min(1.0, reward_value)),
        breakdown=breakdown,
        message="; ".join(messages) if messages else "Action processed"
    )
