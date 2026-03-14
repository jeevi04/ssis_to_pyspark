#!/bin/bash
# Development environment setup script

set -e

echo "🚀 Setting up SSIS to PySpark development environment..."

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
required_version="3.11"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python 3.11+ is required. Found: $python_version"
    exit 1
fi
echo "✅ Python version: $python_version"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi
echo "✅ uv is installed"

# Install dependencies
echo "📦 Installing dependencies..."
uv sync --dev

# Copy environment file if not exists
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✅ Created .env file (please configure API keys if needed)"
else
    echo "✅ .env file already exists"
fi

# Check Ollama
if command -v ollama &> /dev/null; then
    echo "✅ Ollama is installed"
    
    # Check if model is available
    if ollama list | grep -q "llama3.2"; then
        echo "✅ llama3.2 model is available"
    else
        echo "⚠️  llama3.2 model not found. Install with: ollama pull llama3.2"
    fi
else
    echo "⚠️  Ollama not found. Install from: https://ollama.ai/download"
fi

# Create necessary directories
mkdir -p logs output

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Configure .env if using cloud LLM providers"
echo "  2. Start Ollama: ollama serve"
echo "  3. Test the CLI: uv run ssis2spark --help"
echo "  4. Run tests: uv run pytest"
