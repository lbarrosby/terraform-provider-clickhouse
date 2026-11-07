package resource

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/terraform-provider-clickhouse/internal/api"
)

// staleThenFreshGetRole returns the stale actors for the first staleReads
// calls and the fresh actors afterwards, like an eventually consistent API.
func staleThenFreshGetRole(staleReads int, stale, fresh []string, calls *int) func(context.Context, string) (*api.RBACRole, error) {
	return func(_ context.Context, roleID string) (*api.RBACRole, error) {
		*calls++
		if *calls <= staleReads {
			return &api.RBACRole{ID: roleID, Actors: stale}, nil
		}
		return &api.RBACRole{ID: roleID, Actors: fresh}, nil
	}
}

func roleAssignmentSchema(ctx context.Context, t *testing.T, r *RoleAssignmentResource) tfsdk.State {
	t.Helper()
	schemaResp := &resource.SchemaResponse{}
	r.Schema(ctx, resource.SchemaRequest{}, schemaResp)
	require.False(t, schemaResp.Diagnostics.HasError(), "building resource schema failed: %v", schemaResp.Diagnostics.Errors())
	return tfsdk.State{Schema: schemaResp.Schema}
}

func TestRoleAssignmentResource_Create_WaitsForActorsToPropagate(t *testing.T) {
	ctx := context.Background()
	mc := minimock.NewController(t)

	calls := 0
	client := api.NewClientMock(mc).
		UpdateRoleMock.Return(&api.RBACRole{ID: "role-1"}, nil).
		GetRoleMock.Set(staleThenFreshGetRole(2, []string{}, []string{"user/user-1"}, &calls))

	r := &RoleAssignmentResource{client: client, actorsPollInterval: time.Millisecond, actorsMaxRetries: 5}
	empty := roleAssignmentSchema(ctx, t, r)

	plan := RoleAssignmentModel{
		ID:        types.StringUnknown(),
		RoleID:    types.StringValue("role-1"),
		UserIDs:   strSetValue("user-1"),
		APIKeyIDs: types.SetNull(types.StringType),
	}
	planVal := tfsdk.Plan{Schema: empty.Schema}
	require.False(t, planVal.Set(ctx, &plan).HasError())

	resp := &resource.CreateResponse{State: tfsdk.State{Schema: empty.Schema}}
	r.Create(ctx, resource.CreateRequest{Plan: planVal}, resp)
	require.False(t, resp.Diagnostics.HasError(), "create failed: %v", resp.Diagnostics.Errors())

	var got RoleAssignmentModel
	require.False(t, resp.State.Get(ctx, &got).HasError())
	require.True(t, got.UserIDs.Equal(strSetValue("user-1")), "user_ids = %v, want [user-1]", got.UserIDs)
}

func TestRoleAssignmentResource_Update_WaitsForActorsToPropagate(t *testing.T) {
	ctx := context.Background()
	mc := minimock.NewController(t)

	calls := 0
	client := api.NewClientMock(mc).
		UpdateRoleMock.Return(&api.RBACRole{ID: "role-1"}, nil).
		GetRoleMock.Set(staleThenFreshGetRole(1, []string{"user/user-1"}, []string{"user/user-1", "user/user-2"}, &calls))

	r := &RoleAssignmentResource{client: client, actorsPollInterval: time.Millisecond, actorsMaxRetries: 5}
	empty := roleAssignmentSchema(ctx, t, r)

	state := RoleAssignmentModel{
		ID:        types.StringValue("role-1"),
		RoleID:    types.StringValue("role-1"),
		UserIDs:   strSetValue("user-1"),
		APIKeyIDs: types.SetNull(types.StringType),
	}
	plan := state
	plan.UserIDs = strSetValue("user-1", "user-2")

	stateVal := tfsdk.State{Schema: empty.Schema}
	require.False(t, stateVal.Set(ctx, &state).HasError())
	planVal := tfsdk.Plan{Schema: empty.Schema}
	require.False(t, planVal.Set(ctx, &plan).HasError())

	resp := &resource.UpdateResponse{State: tfsdk.State{Schema: empty.Schema}}
	r.Update(ctx, resource.UpdateRequest{State: stateVal, Plan: planVal}, resp)
	require.False(t, resp.Diagnostics.HasError(), "update failed: %v", resp.Diagnostics.Errors())

	var got RoleAssignmentModel
	require.False(t, resp.State.Get(ctx, &got).HasError())
	require.True(t, got.UserIDs.Equal(strSetValue("user-1", "user-2")), "user_ids = %v, want [user-1 user-2]", got.UserIDs)
}

func TestRoleAssignmentResource_waitForActors(t *testing.T) {
	ctx := context.Background()

	t.Run("gives up after the retries and keeps the last read", func(t *testing.T) {
		mc := minimock.NewController(t)
		calls := 0
		client := api.NewClientMock(mc).
			GetRoleMock.Set(staleThenFreshGetRole(100, []string{}, []string{"user/user-1"}, &calls))
		r := &RoleAssignmentResource{client: client, actorsPollInterval: time.Millisecond, actorsMaxRetries: 3}

		require.NoError(t, r.waitForActors(ctx, "role-1", []string{"user/user-1"}))
		require.Equal(t, 4, calls)
	})

	t.Run("returns API errors without retrying", func(t *testing.T) {
		mc := minimock.NewController(t)
		calls := 0
		client := api.NewClientMock(mc).
			GetRoleMock.Set(func(context.Context, string) (*api.RBACRole, error) {
			calls++
			return nil, fmt.Errorf("status: 500, body: internal error")
		})
		r := &RoleAssignmentResource{client: client, actorsPollInterval: time.Millisecond, actorsMaxRetries: 3}

		require.Error(t, r.waitForActors(ctx, "role-1", []string{"user/user-1"}))
		require.Equal(t, 1, calls)
	})

	t.Run("ignores actor order", func(t *testing.T) {
		mc := minimock.NewController(t)
		calls := 0
		client := api.NewClientMock(mc).
			GetRoleMock.Set(staleThenFreshGetRole(0, nil, []string{"apiKey/key-1", "user/user-1"}, &calls))
		r := &RoleAssignmentResource{client: client, actorsPollInterval: time.Millisecond, actorsMaxRetries: 3}

		require.NoError(t, r.waitForActors(ctx, "role-1", []string{"user/user-1", "apiKey/key-1"}))
		require.Equal(t, 1, calls)
	})
}
