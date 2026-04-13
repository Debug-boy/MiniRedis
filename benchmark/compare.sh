#!/bin/bash

# 定义并发数列表
CONCURRENCIES=(50 100 200 400 600 800 1000 1500 3000 5000 10000)

# 定义端口和对应的日志文件
# 请确保 redis-benchmark 在你的环境变量 PATH 中，或者修改下面的变量为绝对路径
BENCHMARK_TOOL="redis-benchmark"
PORT_YOUR=9736
LOG_YOUR="result_9736.log"
PORT_OFFICIAL=6379
LOG_OFFICIAL="result_6379.log"

# 检查工具是否存在
if ! command -v $BENCHMARK_TOOL &> /dev/null; then
    echo "❌ 错误: 未找到 redis-benchmark 工具。请确保已安装 Redis 并将其添加到 PATH 中。"
    exit 1
fi

echo "🚀 开始 Redis 性能对比基准测试..."
echo "📊 测试命令: SET/GET, 默认请求数: 100000"
echo "----------------------------------------"

# 函数：执行测试
run_test() {
    local port=$1
    local log_file=$2
    local concurrency=$3
    
    # 执行命令并将输出追加到日志文件
    # -t set,get: 只测试 set 和 get
    # -q: 安静模式，只显示 QPS
    # -c: 并发数
    # -p: 端口
    $BENCHMARK_TOOL -t set,get -q -p $port -c $concurrency | while read line; do
        echo "[Port $port][C $concurrency] $line" | tee -a $log_file
    done
}

# 1. 测试你的 Redis 程序 (端口 9736)
echo "🔵 正在测试你的 Redis (Port $PORT_YOUR)..."
# 清空旧日志
> $LOG_YOUR
for c in "${CONCURRENCIES[@]}"
do
    echo "  -> 正在运行并发数: $c"
    run_test $PORT_YOUR $LOG_YOUR $c
done
echo "✅ 你的 Redis 测试完成，结果已保存至 $LOG_YOUR"
echo "----------------------------------------"

# 2. 测试官方 Redis 8.0 (端口 6379)
echo "🟠 正在测试官方 Redis 8.0 (Port $PORT_OFFICIAL)..."
# 清空旧日志
> $LOG_OFFICIAL
for c in "${CONCURRENCIES[@]}"
do
    echo "  -> 正在运行并发数: $c"
    run_test $PORT_OFFICIAL $LOG_OFFICIAL $c
done
echo "✅ 官方 Redis 测试完成，结果已保存至 $LOG_OFFICIAL"
echo "----------------------------------------"

echo "🎉 所有测试已结束！"
echo "💡 提示：你可以使用 grep 命令快速对比结果，例如："
echo "   grep 'SET:' $LOG_YOUR"
echo "   grep 'SET:' $LOG_OFFICIAL"