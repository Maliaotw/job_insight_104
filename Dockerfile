FROM python:3.11.9-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 uv
RUN pip install uv

WORKDIR /app

# 複製所有文件
COPY . .

# 安裝 Python 依賴 (根據存在的文件選擇)
RUN if [ -f "requirements.txt" ]; then \
        pip install --no-cache-dir -r requirements.txt; \
    elif [ -f "pyproject.toml" ]; then \
        pip install uv && uv sync --no-dev || pip install -e .; \
    else \
        pip install streamlit pandas numpy plotly seaborn matplotlib boto3 requests pyyaml click; \
    fi


# 设置环境变量
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

# 创建数据目录
RUN mkdir -p /app/data

# 设置权限
RUN chmod +x bin/*.py

EXPOSE 8501

# 使用 uv 运行应用
#CMD ["uv", "run", "bin/run_analysis_app.py"]
CMD ["python", "bin/run_analysis_app.py"]