# 命令行程序

Runner是DP自动化框架的命令行程序


## CLI 用法
  
	# 帮助文档  
	py -3 runner.py -h  
	  
	# 执行测试计划，多个之间加空格
	py -3 runner.py -p plan_name
	  
	# 执行测试脚本，多个之间加空格  
	py -3 runner.py -c case_name  
	  
	# 发送结果通知，多个通知器之间加空格    
	py -3 runner.py [-p/-c] -n notifier_name  

  
## CLI 退出状态码说明  

- **exit 0:** 测试执行正确，没有异常  
- **exit 1:** Runner初始化失败，或其他框架未捕获异常  
- **exit 2:** 回归测试失败  
- **exit 4:** 结果清理步骤失败  
- **exit 8:** 结果通知失败