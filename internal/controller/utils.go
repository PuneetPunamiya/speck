package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// updateStatusAfterCreation updates the SnowflakeAccount status after successful account creation
func (r *SnowflakeAccountReconciler) updateStatusAfterCreation(ctx context.Context, snowflakeAccount *operatorv1alpha1.SnowflakeAccount, details *accountDetails) error {
	log := logf.FromContext(ctx)

	// Update status fields
	snowflakeAccount.Status.AccountCreated = true
	snowflakeAccount.Status.AccountURL = fmt.Sprintf("https://%s.snowflakecomputing.com", details.accountName)
	snowflakeAccount.Status.Message = "Snowflake account created successfully"
	now := metav1.Now()
	snowflakeAccount.Status.CreationTime = &now

	// Persist the status update
	if err := r.Status().Update(ctx, snowflakeAccount); err != nil {
		log.Error(err, "Failed to update status after account creation")
		return err
	}

	return nil
}

// generateRandomAccountName generates a random account name (8 uppercase alphanumeric characters)
func generateRandomAccountName() string {
	return "SF" + generateRandomString(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}

// generateRandomUsername generates a random username
func generateRandomUsername() string {
	return "admin_" + generateRandomString(8, "abcdefghijklmnopqrstuvwxyz0123456789")
}

// generateRandomPassword generates a secure random password
func generateRandomPassword() string {
	// Password with uppercase, lowercase, numbers, and special characters
	upper := generateRandomString(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	lower := generateRandomString(4, "abcdefghijklmnopqrstuvwxyz")
	numbers := generateRandomString(4, "0123456789")
	special := generateRandomString(2, "!@#$%^&*")

	// Combine and shuffle
	password := upper + lower + numbers + special
	return shuffleString(password)
}

// generateRandomString generates a random string of specified length from the given charset
func generateRandomString(length int, charset string) string {
	result := make([]byte, length)
	charsetLen := big.NewInt(int64(len(charset)))

	for i := 0; i < length; i++ {
		num, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			// Fallback to a simple character if random fails
			result[i] = charset[i%len(charset)]
			continue
		}
		result[i] = charset[num.Int64()]
	}

	return string(result)
}

// shuffleString shuffles the characters in a string
func shuffleString(s string) string {
	runes := []rune(s)
	n := len(runes)

	for i := n - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			continue
		}
		j := int(jBig.Int64())
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes)
}

// extractAccountNameFromURL extracts the account name from a Snowflake account URL
// Expected format: https://{accountName}.snowflakecomputing.com
func extractAccountNameFromURL(url string) string {
	if url == "" {
		return ""
	}

	// Remove https:// prefix if present
	if len(url) > 8 && url[:8] == "https://" {
		url = url[8:]
	}

	// Find the first dot to extract the account name
	for i, ch := range url {
		if ch == '.' {
			return url[:i]
		}
	}

	return ""
}

// checkDuration checks if the account has exceeded its duration and should be deleted
// Returns (shouldDelete, requeueAfter)
func (r *SnowflakeAccountReconciler) checkDuration(ctx context.Context, snowflakeAccount *operatorv1alpha1.SnowflakeAccount) (bool, time.Duration) {
	log := logf.FromContext(ctx)

	// If no creation time is set, don't delete
	if snowflakeAccount.Status.CreationTime == nil {
		log.Info("No creation time set, skipping duration check")
		return false, 0
	}

	// Parse the duration from spec (default to 2 minutes)
	durationStr := snowflakeAccount.Spec.Duration
	if durationStr == "" {
		durationStr = "2m"
	}

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		log.Error(err, "Failed to parse duration, using default 2m", "duration", durationStr)
		duration = 2 * time.Minute
	}

	// Calculate when the account should be deleted
	creationTime := snowflakeAccount.Status.CreationTime.Time
	expirationTime := creationTime.Add(duration)
	currentTime := r.Clock.Now()

	// Check if duration has expired
	if currentTime.After(expirationTime) {
		log.Info("Duration has expired",
			"creationTime", creationTime,
			"expirationTime", expirationTime,
			"currentTime", currentTime,
			"duration", duration)
		return true, 0
	}

	// Calculate how long until expiration
	timeUntilExpiration := expirationTime.Sub(currentTime)
	log.Info("Duration not yet expired",
		"creationTime", creationTime,
		"expirationTime", expirationTime,
		"currentTime", currentTime,
		"timeUntilExpiration", timeUntilExpiration)

	// Return false but suggest requeue time
	return false, timeUntilExpiration
}
