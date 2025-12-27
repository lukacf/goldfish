"""TDD tests for prompt escaping to prevent injection attacks.

These tests verify that user-provided content is properly escaped before
being included in AI prompts to prevent XML/HTML injection attacks.

The escape_for_prompt function should be added to pre_run_review.py to
escape user content in REVIEW_PROMPT and STAGE_SECTION_TEMPLATE.
"""

from __future__ import annotations


class TestEscapeForPromptBasics:
    """Test basic XML/HTML character escaping."""

    def test_escape_less_than(self) -> None:
        """Escape < to &lt; to prevent XML tag injection."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("if x < 5:")
        assert result == "if x &lt; 5:"
        assert "<" not in result

    def test_escape_greater_than(self) -> None:
        """Escape > to &gt; to prevent XML tag closing."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("if x > 5:")
        assert result == "if x &gt; 5:"
        assert ">" not in result

    def test_escape_ampersand(self) -> None:
        """Escape & to &amp; to prevent entity injection."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("save & exit")
        assert result == "save &amp; exit"
        # Count raw & characters (not in entity sequences)
        assert result.count("&amp;") == 1

    def test_escape_double_quote(self) -> None:
        """Escape double quotes to &quot; to prevent attribute injection."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt('name = "value"')
        assert result == "name = &quot;value&quot;"
        assert '"' not in result

    def test_escape_single_quote(self) -> None:
        """Escape single quotes to &#39; to prevent attribute injection."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("name = 'value'")
        assert result == "name = &#39;value&#39;"
        assert "'" not in result

    def test_escape_all_special_chars(self) -> None:
        """Escape all special characters in one string."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = """<tag attr="value" other='val'>data & more</tag>"""
        result = escape_for_prompt(input_text)
        assert "&lt;tag" in result
        assert "&quot;value&quot;" in result
        assert "&#39;val&#39;" in result
        assert "&amp;" in result
        assert "&lt;/tag&gt;" in result
        # Verify no unescaped special chars
        assert "<" not in result
        assert ">" not in result
        assert '"' not in result
        assert "'" not in result


class TestEscapeForPromptEdgeCases:
    """Test edge cases and special scenarios."""

    def test_escape_empty_string(self) -> None:
        """Empty string should return empty string."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("")
        assert result == ""

    def test_escape_no_special_chars(self) -> None:
        """String without special chars should be unchanged."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = "def train_model(epochs):"
        result = escape_for_prompt(input_text)
        assert result == input_text

    def test_escape_only_special_chars(self) -> None:
        """String of only special chars should be fully escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("<>&\"'")
        assert result == "&lt;&gt;&amp;&quot;&#39;"

    def test_escape_whitespace_preserved(self) -> None:
        """Whitespace (spaces, tabs, newlines) should be preserved."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = "line 1\n\tindented line 2\n  line 3"
        result = escape_for_prompt(input_text)
        assert result == input_text
        assert "\n" in result
        assert "\t" in result


class TestEscapeForPromptAlreadyEscaped:
    """Test handling of already-escaped content."""

    def test_double_escape_ampersand(self) -> None:
        """Already escaped ampersand should be double-escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        # If user provides "&lt;" as literal text, it should become "&amp;lt;"
        # This prevents confusion between user content and markup
        result = escape_for_prompt("&lt;")
        assert result == "&amp;lt;"

    def test_double_escape_complete_entity(self) -> None:
        """Already escaped entity should be double-escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        result = escape_for_prompt("&quot;test&quot;")
        assert result == "&amp;quot;test&amp;quot;"

    def test_partial_entity_escaped(self) -> None:
        """Partial entity-like strings should be escaped normally."""
        from goldfish.pre_run_review import escape_for_prompt

        # "&test" is not a complete entity, so just escape the &
        result = escape_for_prompt("&test")
        assert result == "&amp;test"


class TestEscapeForPromptUnicode:
    """Test Unicode handling."""

    def test_escape_unicode_chars_preserved(self) -> None:
        """Unicode characters without special meaning should be preserved."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = "Model: 测试 模型 🚀"
        result = escape_for_prompt(input_text)
        assert result == input_text
        assert "测试" in result
        assert "🚀" in result

    def test_escape_unicode_with_special_chars(self) -> None:
        """Unicode mixed with special chars should escape only special chars."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = "データ <tag> 値"
        result = escape_for_prompt(input_text)
        assert "データ" in result
        assert "値" in result
        assert "&lt;tag&gt;" in result
        assert "<" not in result

    def test_escape_emoji_with_quotes(self) -> None:
        """Emoji with quotes should preserve emoji and escape quotes."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = '🎯 "target" 🚀'
        result = escape_for_prompt(input_text)
        assert "🎯" in result
        assert "🚀" in result
        assert "&quot;target&quot;" in result


class TestEscapeForPromptRealWorldScenarios:
    """Test real-world scenarios from ML experiment contexts."""

    def test_escape_file_path(self) -> None:
        """File paths should be escaped (though typically no special chars)."""
        from goldfish.pre_run_review import escape_for_prompt

        input_text = "/path/to/file.py"
        result = escape_for_prompt(input_text)
        assert result == input_text

    def test_escape_python_code_with_comparison(self) -> None:
        """Python code with comparison operators should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        code = "if x < 10 and y > 5:\n    return True"
        result = escape_for_prompt(code)
        assert "x &lt; 10" in result
        assert "y &gt; 5" in result
        assert "\n" in result  # Newlines preserved

    def test_escape_yaml_config_with_quotes(self) -> None:
        """YAML config with quotes should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        config = 'model: "bert-base"\nlr: 0.001'
        result = escape_for_prompt(config)
        assert "&quot;bert-base&quot;" in result
        assert "lr: 0.001" in result  # Unchanged part

    def test_escape_diff_with_brackets(self) -> None:
        """Git diff with angle brackets should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        diff = "+from typing import List[Dict[str, Any]]"
        result = escape_for_prompt(diff)
        assert "List&lt;Dict&lt;str" not in result  # Should use [ not <
        # But if user did use angle brackets:
        diff2 = "+template <typename T>"
        result2 = escape_for_prompt(diff2)
        assert "&lt;typename T&gt;" in result2

    def test_escape_html_like_content(self) -> None:
        """HTML-like user content should be fully escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        # A malicious user might try to inject XML
        injection = "<script>alert('xss')</script>"
        result = escape_for_prompt(injection)
        assert "&lt;script&gt;" in result
        assert "&lt;/script&gt;" in result
        assert "<script>" not in result
        assert "alert(&#39;xss&#39;)" in result

    def test_escape_xml_processing_instruction(self) -> None:
        """XML processing instructions should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        xml_pi = "<?xml version='1.0'?>"
        result = escape_for_prompt(xml_pi)
        assert "&lt;?xml" in result
        assert "version=&#39;1.0&#39;" in result
        assert "?&gt;" in result

    def test_escape_run_reason_description(self) -> None:
        """RunReason descriptions should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        # User might describe: "Testing if accuracy > 90%"
        description = "Test if accuracy > 90% & recall > 85%"
        result = escape_for_prompt(description)
        assert "accuracy &gt; 90%" in result
        assert "&amp; recall &gt; 85%" in result

    def test_escape_error_message_with_traceback(self) -> None:
        """Error messages with < and > should be escaped."""
        from goldfish.pre_run_review import escape_for_prompt

        error = "TypeError: expected '<' not '>=' in comparison"
        result = escape_for_prompt(error)
        assert "&#39;&lt;&#39;" in result
        assert "&#39;&gt;=&#39;" in result


class TestEscapeForPromptLongContent:
    """Test performance and correctness with longer content."""

    def test_escape_multiline_code(self) -> None:
        """Multi-line code blocks should be escaped correctly."""
        from goldfish.pre_run_review import escape_for_prompt

        code = """def compare(a, b):
    if a < b:
        return "<"
    elif a > b:
        return ">"
    return "="
"""
        result = escape_for_prompt(code)
        assert "if a &lt; b:" in result
        assert "return &quot;&lt;&quot;" in result  # Both " and < are escaped
        assert "elif a &gt; b:" in result
        assert "return &quot;&gt;&quot;" in result  # Both " and > are escaped
        assert 'return "="' not in result  # Should be escaped
        assert "return &quot;=&quot;" in result

    def test_escape_large_file_content(self) -> None:
        """Large content should be escaped efficiently."""
        from goldfish.pre_run_review import escape_for_prompt

        # Simulate a large file with special chars
        large_content = "x < 5\n" * 1000  # 1000 lines
        result = escape_for_prompt(large_content)
        assert result.count("&lt;") == 1000
        assert "<" not in result
        assert len(result) > len(large_content)  # Should be longer due to escaping

    def test_escape_preserves_structure(self) -> None:
        """Escaping should preserve overall structure and readability."""
        from goldfish.pre_run_review import escape_for_prompt

        structured = """Stage: preprocess
Input: <dataset>
Output: {type: 'csv'}
Logic:
  - Filter where x > 0
  - Transform "column_a" & "column_b"
"""
        result = escape_for_prompt(structured)
        # Structure should remain readable
        assert "Stage: preprocess" in result
        assert "&lt;dataset&gt;" in result
        assert "type: &#39;csv&#39;" in result
        assert "x &gt; 0" in result
        assert "&quot;column_a&quot; &amp; &quot;column_b&quot;" in result


class TestEscapeForPromptIntegration:
    """Test that escaping integrates properly with prompt building."""

    def test_escape_in_review_prompt_context(self) -> None:
        """Escaped content should work in actual prompt templates."""
        from goldfish.pre_run_review import escape_for_prompt

        # Simulate what would go into REVIEW_PROMPT
        workspace_name = "test<script>"
        diff_text = '+print("hello & goodbye")'
        run_reason = "Test accuracy > 95%"

        escaped_workspace = escape_for_prompt(workspace_name)
        escaped_diff = escape_for_prompt(diff_text)
        escaped_reason = escape_for_prompt(run_reason)

        # Build a fragment similar to REVIEW_PROMPT
        fragment = f"Workspace: {escaped_workspace}\nDiff: {escaped_diff}\nReason: {escaped_reason}"

        # Verify no injection possible
        assert "&lt;script&gt;" in fragment
        assert "&quot;hello &amp; goodbye&quot;" in fragment
        assert "&gt; 95%" in fragment
        assert "<" not in fragment
        assert ">" not in fragment

    def test_escape_in_stage_section_context(self) -> None:
        """Escaped content should work in stage section templates."""
        from goldfish.pre_run_review import escape_for_prompt

        # Simulate stage section content
        stage_name = "train<tag>"
        module_content = "if loss < threshold:\n    print('success')"
        config_content = "lr: 0.001\noptimizer: 'adam'"

        escaped_stage = escape_for_prompt(stage_name)
        escaped_module = escape_for_prompt(module_content)
        escaped_config = escape_for_prompt(config_content)

        # Build a fragment similar to STAGE_SECTION_TEMPLATE
        fragment = f"## {escaped_stage}\n```python\n{escaped_module}\n```\n```yaml\n{escaped_config}\n```"

        # Verify escaping
        assert "train&lt;tag&gt;" in fragment
        assert "loss &lt; threshold" in fragment
        assert "print(&#39;success&#39;)" in fragment
        assert "optimizer: &#39;adam&#39;" in fragment
