# GitHub 發版

每版在私有狀態下撰寫 `docs/releases/vX.Y.Z.md`，隨版本提交與標籤保存。正文須交代新增功能、優化、Bug 修正、升級影響與實際驗證結果。發版前依 README 檢查說明，發布後核對 GitHub Release 正文與附件。

`holeenlu/tap-codex-kernel` 平時保持私有。只有發行新版才暫時公開：先公開，再推版本標籤、等 GitHub 編譯與發布、核對附件，成功後立即改回私有並驗證。普通提交與推送不開啟公開窗口。發版由執行者負責追蹤到收回可見性；具體命令、完成條件及異常收口依 [README 的發布規則](README.md#發布)。
