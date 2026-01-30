"""Regression tests for skip_review parameter in async mode.

Bug context: skip_review=True was ignored in async mode because it wasn't
passed through _worker_loop and _process_pipeline_queue_once to run_stage().
"""

from unittest.mock import MagicMock, patch


def test_skip_review_passed_to_worker_loop_in_async_mode():
    """Regression test: skip_review must be passed to _pool.submit in async mode.

    Bug: skip_review=True worked in sync mode (wait=True) but was ignored in
    async mode because it wasn't passed to _worker_loop.
    """
    from goldfish.jobs.pipeline_executor import PipelineExecutor

    # Create executor with mocked dependencies
    mock_db = MagicMock()
    mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_db)
    mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

    mock_stage_executor = MagicMock()
    mock_pipeline_manager = MagicMock()

    # Setup pipeline manager to return a valid pipeline
    mock_stage = MagicMock()
    mock_stage.name = "test_stage"
    mock_pipeline = MagicMock()
    mock_pipeline.stages = [mock_stage]
    mock_pipeline_manager.get_pipeline.return_value = mock_pipeline

    executor = PipelineExecutor(
        stage_executor=mock_stage_executor,
        pipeline_manager=mock_pipeline_manager,
        db=mock_db,
    )

    # Patch the thread pool submit to capture the arguments
    with patch.object(executor._pool, "submit") as mock_submit:
        # Call run_stages with skip_review=True in async mode (async_mode=True is default)
        executor.run_stages(
            workspace="test_ws",
            stages=["test_stage"],
            skip_review=True,
            async_mode=True,
        )

        # Verify submit was called
        assert mock_submit.called, "Thread pool submit should be called in async mode"

        # Get the arguments passed to submit
        call_args = mock_submit.call_args
        args = call_args[0]  # positional args

        # The last argument should be skip_review=True
        # Arguments are: _worker_loop, pipeline_run_id, workspace, pipeline_name,
        #                config_override, inputs_override, reason, reason_dict,
        #                results_spec, experiment_group, skip_review
        assert args[-1] is True, f"skip_review should be True, got {args[-1]}"


def test_skip_review_passed_to_run_stage_in_sync_mode():
    """Verify skip_review is passed to run_stage in sync mode."""
    from goldfish.jobs.pipeline_executor import PipelineExecutor

    mock_db = MagicMock()
    mock_stage_executor = MagicMock()
    mock_pipeline_manager = MagicMock()

    # Setup pipeline manager
    mock_stage = MagicMock()
    mock_stage.name = "test_stage"
    mock_pipeline = MagicMock()
    mock_pipeline.stages = [mock_stage]
    mock_pipeline_manager.get_pipeline.return_value = mock_pipeline

    # Setup stage executor to return a mock result
    mock_run_info = MagicMock()
    mock_run_info.model_dump.return_value = {"stage_run_id": "test-123"}
    mock_stage_executor.run_stage.return_value = mock_run_info

    executor = PipelineExecutor(
        stage_executor=mock_stage_executor,
        pipeline_manager=mock_pipeline_manager,
        db=mock_db,
    )

    # Call run_stages with skip_review=True in sync mode
    executor.run_stages(
        workspace="test_ws",
        stages=["test_stage"],
        skip_review=True,
        async_mode=False,
    )

    # Verify run_stage was called with skip_review=True
    mock_stage_executor.run_stage.assert_called_once()
    call_kwargs = mock_stage_executor.run_stage.call_args[1]
    assert call_kwargs.get("skip_review") is True, f"skip_review should be True, got {call_kwargs}"
