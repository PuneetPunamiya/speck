/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
)

// SnowflakeAccountReconciler reconciles a SnowflakeAccount object
type SnowflakeAccountReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock  clock.PassiveClock
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=snowflakeaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=snowflakeaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=snowflakeaccounts/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SnowflakeAccount object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *SnowflakeAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the SnowflakeAccount instance
	snowflakeAccount := &operatorv1alpha1.SnowflakeAccount{}
	err := r.Get(ctx, req.NamespacedName, snowflakeAccount)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("SnowflakeAccount resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get SnowflakeAccount")
		return ctrl.Result{}, err
	}

	// Handle finalizer operations (deletion, adding/removing finalizers)
	continueReconciliation, result, err := r.handleFinalizerOperations(ctx, snowflakeAccount)
	if !continueReconciliation {
		return result, err
	}

	// Check if the account has already been created
	if snowflakeAccount.Status.AccountCreated {
		log.Info("Snowflake account already created")

		// Check if duration has expired
		if shouldDeleteDueToDuration, requeueAfter := r.checkDuration(ctx, snowflakeAccount); shouldDeleteDueToDuration {
			log.Info("Duration expired, deleting Snowflake account")

			// Delete the Kubernetes resource - the finalizer will handle Snowflake account cleanup
			if err := r.Delete(ctx, snowflakeAccount); err != nil {
				log.Error(err, "Failed to delete SnowflakeAccount resource due to duration expiration")
				return ctrl.Result{}, err
			}

			log.Info("Triggered deletion of Snowflake account due to duration expiration")
			return ctrl.Result{}, nil
		} else if requeueAfter > 0 {
			// Requeue to check duration again
			log.Info("Requeuing to check duration", "after", requeueAfter)
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}

		return ctrl.Result{}, nil
	}

	// Create the Snowflake account
	log.Info("Creating Snowflake account")
	accountDetails, err := r.createSnowflakeAccount(ctx, snowflakeAccount)
	if err != nil {
		log.Error(err, "Failed to create Snowflake account")
		snowflakeAccount.Status.Message = fmt.Sprintf("Failed to create account: %v", err)
		if statusErr := r.Status().Update(ctx, snowflakeAccount); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Create a secret to store the credentials
	if err := r.createCredentialsSecret(ctx, snowflakeAccount, accountDetails); err != nil {
		log.Error(err, "Failed to create credentials secret")
		snowflakeAccount.Status.Message = fmt.Sprintf("Account created but failed to store credentials: %v", err)
		if statusErr := r.Status().Update(ctx, snowflakeAccount); statusErr != nil {
			log.Error(statusErr, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Update status to indicate successful creation
	if err := r.updateStatusAfterCreation(ctx, snowflakeAccount, accountDetails); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Successfully created Snowflake account and stored credentials", "accountName", accountDetails.accountName)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnowflakeAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.SnowflakeAccount{}).
		Named("snowflakeaccount").
		Complete(r)
}
