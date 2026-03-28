# 双色球自适应回测与预测结果

## 说明
- 数据源：附件1《双色球最近1000期开奖结果.xlsx》
- 官方增强源：中国福彩网双色球往期开奖接口（https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/）
- 规则源：附件2《福利彩票双色球游戏规则.docx》
- 历史范围：2019069 至 2026031，共 1001 期
- 初始训练窗口：2019069 至 2020017，共 100 期
- 首个滚动预测目标：2020018
- 最终预测目标：2026032（推断开奖日 2026-03-24）
- 中奖金额口径：若当期已匹配到中国福彩网官方奖级金额，则一等奖至六等奖全部按当期官方单注奖金精确回测；缺失时才回退到规则固定奖金。
- 风险提示：双色球开奖本质上接近独立随机事件，以下结果只能作为基于历史拟合的预算优化参考，不能视为确定中奖承诺。

## 引擎进化点
- 红球与蓝球拆分为两套独立在线权重，不再共用同一组策略权重。
- 启发式策略层之外，新增在线二分类模型层，并与启发式层做自适应融合。
- 新增相似期检索特征，用历史上相近局面的后续开奖来增强当前号码打分。
- 相似期上下文已接入官方销售额、奖池、一二等奖注数/金额、一等奖地区分布与固定奖负担等市场维度。
- 预算结构选择改为“历史表现 EMA + 期望回报校准分 + 探索项”共同决定。

## 规则摘录
- 单注 2 元；红球从 1-33 选 6 个，蓝球从 1-16 选 1 个。
- 三等奖：5 红 + 1 蓝，3000 元。
- 四等奖：5 红 或 4 红 + 1 蓝，200 元。
- 五等奖：4 红 或 3 红 + 1 蓝，10 元。
- 六等奖：1 蓝，5 元。

## 回测摘要
- 10元：全奖级 ROI=-0.7944, 总成本=2310, 奖金回收=475, 中奖期命中率=0.0844, 五等奖及以上命中率=0.0067, 四等奖及以上命中率=0.0, 常用结构={'hold': 670, 'blue_fushi': 179, 'single_pack': 52}
- 50元：全奖级 ROI=-0.8074, 总成本=27286, 奖金回收=5255, 中奖期命中率=0.8624, 五等奖及以上命中率=0.0655, 四等奖及以上命中率=0.0022, 常用结构={'hold': 95, 'blue_fushi': 712, 'dantuo': 88, 'single_pack': 3, 'full_fushi': 3}
- 100元：全奖级 ROI=-0.8343, 总成本=57706, 奖金回收=9560, 中奖期命中率=0.3363, 五等奖及以上命中率=0.0433, 四等奖及以上命中率=0.0011, 常用结构={'hold': 318, 'dantuo': 403, 'full_fushi': 147, 'single_pack': 33}
- 200元：全奖级 ROI=-0.8129, 总成本=88400, 奖金回收=16540, 中奖期命中率=0.2697, 五等奖及以上命中率=0.04, 四等奖及以上命中率=0.0033, 常用结构={'hold': 459, 'dantuo': 422, 'single_pack': 20}
- 500元：全奖级 ROI=-0.7937, 总成本=289524, 奖金回收=59735, 中奖期命中率=0.4162, 五等奖及以上命中率=0.0755, 四等奖及以上命中率=0.0111, 常用结构={'hold': 287, 'dantuo': 429, 'single_pack': 14, 'full_fushi': 171}
- 1000元：全奖级 ROI=-0.7809, 总成本=169176, 奖金回收=37060, 中奖期命中率=0.081, 五等奖及以上命中率=0.0255, 四等奖及以上命中率=0.0044, 常用结构={'hold': 721, 'dantuo': 150, 'full_fushi': 30}

## 高回报命中期数
- 2020122期：回收=12310，成本=1492，收益=10818，ROI=7.25067，命中方案=50元 blue_fushi blue_fushi::6+16:shift1 回收10; 500元 dantuo dantuo::3胆6拖+蓝12 回收3200; 1000元 dantuo dantuo::2胆8拖+蓝7:hit_warm 回收9100
- 2024068期：回收=6110，成本=712，收益=5398，ROI=7.581461，命中方案=50元 blue_fushi blue_fushi::6+16:shift1|state_bluesrc_heuristic|hit_hot 回收10; 200元 dantuo dantuo::4胆5拖+蓝10:state_redarm_cold_rebound__bluearm_mean_revert 回收2900; 500元 dantuo dantuo::3胆6拖+蓝12:hit_hot 回收3200
- 2021087期：回收=3885，成本=812，收益=3073，ROI=3.784483，命中方案=50元 blue_fushi blue_fushi::6+16:shift2|state_bluesrc_heuristic 回收5; 100元 dantuo dantuo::5胆5拖+蓝10:state_bluearm_mean_revert|hit_hot 回收100; 200元 dantuo dantuo::4胆5拖+蓝10:state_bluearm_mean_revert|hit_warm 回收300
- 2024102期：回收=3175，成本=1658，收益=1517，ROI=0.914958，命中方案=10元 blue_fushi blue_fushi::6+5:shift2|state_bluesrc_heuristic 回收10; 50元 blue_fushi blue_fushi::6+16:core|state_bluesrc_heuristic|hit_hot 回收5; 100元 dantuo dantuo::5胆4拖+蓝12:state_bluearm_mean_revert|hit_warm 回收20
- 2025138期：回收=1845，成本=580，收益=1265，ROI=2.181034，命中方案=50元 blue_fushi blue_fushi::6+16:shift2|state_bluearm_mean_revert|hit_hot 回收10; 100元 dantuo dantuo::5胆5拖+蓝10:state_bluearm_mean_revert|hit_hot 回收25; 500元 full_fushi full_fushi::8+8:shift2|state_redsrc_heuristic|form_confirmed 回收1810
- 2026027期：回收=1685，成本=1320，收益=365，ROI=0.276515，命中方案=50元 blue_fushi blue_fushi::6+16:core|state_bluearm_mean_revert__bluesrc_heuristic|hit_warm 回收10; 500元 full_fushi full_fushi::8+8:shift2|state_redsrc_heuristic|form_confirmed 回收190; 1000元 dantuo dantuo::2胆7拖+蓝12 回收1485
- 2021027期：回收=1330，成本=608，收益=722，ROI=1.1875，命中方案=50元 blue_fushi blue_fushi::6+16:core 回收10; 100元 dantuo dantuo::5胆4拖+蓝12 回收120; 500元 dantuo dantuo::3胆6拖+蓝12:state_bluearm_mean_revert|hit_warm 回收1200
- 2020097期：回收=1320，成本=1662，收益=-342，ROI=-0.205776，命中方案=10元 blue_fushi blue_fushi::6+5:shift1|state_bluefocus_tight 回收50; 50元 blue_fushi blue_fushi::6+16:core|state_bluefocus_tight 回收10; 100元 single_pack single_pack::50:blue_spread|state_bluearm_mean_revert 回收180
- 2020095期：回收=1280，成本=1792，收益=-512，ROI=-0.285714，命中方案=50元 blue_fushi blue_fushi::6+16:shift1 回收10; 100元 dantuo dantuo::5胆5拖+蓝10 回收30; 200元 dantuo dantuo::4胆5拖+蓝10 回收365
- 2020102期：回收=750，成本=1552，收益=-802，ROI=-0.516753，命中方案=50元 blue_fushi blue_fushi::6+16:shift1 回收10; 200元 dantuo dantuo::4胆5拖+蓝10 回收70; 500元 dantuo dantuo::3胆6拖+蓝12 回收150

## 官方同步情况
- 官方记录数：1001，Excel 覆盖率：1.0
- 追加到训练集的新官方期数：1，当前已训练到：2026031
- 官方数据来源状态：live

## 最终推荐
### 10元预算
- 推荐结构：空仓观望
- 实际成本：0 元
- 动作：本期空仓，等待更强信号。

### 50元预算
- 推荐结构：蓝球复式 6+16（稳态核心）（state_bluefocus_tight）（历史确认）
- 实际成本：32 元
- 红球：02 09 13 18 22 25
- 蓝球池：01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16

### 100元预算
- 推荐结构：空仓观望
- 实际成本：0 元
- 动作：本期空仓，等待更强信号。

### 200元预算
- 推荐结构：空仓观望
- 实际成本：0 元
- 动作：本期空仓，等待更强信号。

### 500元预算
- 推荐结构：空仓观望
- 实际成本：0 元
- 动作：本期空仓，等待更强信号。

### 1000元预算
- 推荐结构：空仓观望
- 实际成本：0 元
- 动作：本期空仓，等待更强信号。

## 规则文档核对
- 已从 DOCX 中提取到 3895 个字符，用于核对投注与奖级规则。

## 输出文件
- 预算摘要：/Users/z/Downloads/Codex/outputs/ssq_agent/budget_summary.csv
- 滚动回测明细：/Users/z/Downloads/Codex/outputs/ssq_agent/rolling_backtest.csv
- 高回报期数摘要：/Users/z/Downloads/Codex/outputs/ssq_agent/top_reward_issues.json
- 下一期详细方案：/Users/z/Downloads/Codex/outputs/ssq_agent/prediction_next_issue.json
- 下一期票号清单：/Users/z/Downloads/Codex/outputs/ssq_agent/prediction_tickets.csv
- 引擎状态：/Users/z/Downloads/Codex/outputs/ssq_agent/engine_state.json
- 自适应诊断：/Users/z/Downloads/Codex/outputs/ssq_agent/adaptive_diagnostics.json
- 官方增强摘要：/Users/z/Downloads/Codex/outputs/ssq_agent/official_enrichment.json
- 官方抓取缓存：/Users/z/Downloads/Codex/outputs/ssq_agent/official_draws.json