package gui

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/connector"
)

var Version = "dev"

// App GUI 应用
type App struct {
	fyneApp      fyne.App
	window       fyne.Window
	registry     *connector.Registry
	resultLabel  *widget.Label
	resultScroll *container.Scroll
	pageCache    map[config.ServiceType]fyne.CanvasObject // 缓存已创建的服务页面
}

// NewApp 创建 GUI 应用
func NewApp() *App {
	a := app.New()
	a.Settings().SetTheme(theme.DarkTheme())

	w := a.NewWindow("Connectest - 多服务连接测试工具")
	w.Resize(fyne.NewSize(1100, 700))
	w.CenterOnScreen()

	// 结果区全局共享，切换服务不会丢失
	resultLabel := widget.NewLabel("等待执行...")
	resultLabel.Wrapping = fyne.TextWrapWord
	resultScroll := container.NewVScroll(resultLabel)
	resultScroll.SetMinSize(fyne.NewSize(0, 150))

	return &App{
		fyneApp:      a,
		window:       w,
		registry:     connector.NewRegistry(),
		resultLabel:  resultLabel,
		resultScroll: resultScroll,
		pageCache:    make(map[config.ServiceType]fyne.CanvasObject),
	}
}

// Run 启动 GUI
func (a *App) Run() {
	// 可切换的服务内容区
	serviceContent := container.NewStack()
	serviceContent.Objects = []fyne.CanvasObject{a.createWelcomePage()}

	// 结果区清空按钮
	clearBtn := widget.NewButton("🗑 清空", func() {
		a.resultLabel.SetText("")
	})
	clearBtn.Importance = widget.LowImportance

	resultCard := container.NewBorder(
		container.NewHBox(widget.NewLabel("📋 执行结果:"), layout.NewSpacer(), clearBtn),
		nil, nil, nil,
		a.resultScroll,
	)

	// 右侧：上方服务内容 + 下方固定结果区
	rightPanel := container.NewBorder(nil, resultCard, nil, nil, serviceContent)

	// 左侧服务列表
	serviceList := a.createServiceList(serviceContent)

	versionLabel := widget.NewLabel("Connectest " + Version)
	versionLabel.TextStyle = fyne.TextStyle{Italic: true}

	leftPanel := container.NewBorder(
		widget.NewLabel("  服务列表"),
		container.NewPadded(versionLabel),
		nil, nil,
		serviceList,
	)

	// 分割布局
	split := container.NewHSplit(leftPanel, rightPanel)
	split.Offset = 0.2

	a.window.SetContent(split)
	a.window.ShowAndRun()
}

func (a *App) createWelcomePage() fyne.CanvasObject {
	title := widget.NewLabel("🔗 Connectest")
	title.TextStyle = fyne.TextStyle{Bold: true}
	title.Alignment = fyne.TextAlignCenter

	desc := widget.NewLabel("多服务连接测试工具\n\n支持的服务:")
	desc.Alignment = fyne.TextAlignCenter

	services := ""
	for _, svc := range config.AllServices() {
		services += "• " + svc.Name + "\n"
	}
	svcLabel := widget.NewLabel(services)
	svcLabel.Alignment = fyne.TextAlignCenter

	hint := widget.NewLabel("← 请从左侧选择要测试的服务")
	hint.Alignment = fyne.TextAlignCenter
	hint.TextStyle = fyne.TextStyle{Italic: true}

	return container.NewVBox(
		layout.NewSpacer(),
		title,
		widget.NewSeparator(),
		desc,
		svcLabel,
		widget.NewSeparator(),
		hint,
		layout.NewSpacer(),
	)
}

func (a *App) createServiceList(serviceContent *fyne.Container) fyne.CanvasObject {
	var buttons []*widget.Button
	var selectedBtn *widget.Button

	for _, svc := range config.AllServices() {
		svc := svc // capture
		btn := widget.NewButton(svc.Name, func() {
			if selectedBtn != nil {
				selectedBtn.Importance = widget.MediumImportance
				selectedBtn.Refresh()
			}
			selectedBtn = findButton(buttons, svc.Name)
			if selectedBtn != nil {
				selectedBtn.Importance = widget.HighImportance
				selectedBtn.Refresh()
			}

			// 复用缓存的页面，保留用户填写的表单值
			page, ok := a.pageCache[svc.Type]
			if !ok {
				page = a.createServiceTop(svc)
				a.pageCache[svc.Type] = page
			}
			serviceContent.Objects = []fyne.CanvasObject{page}
			serviceContent.Refresh()
		})
		btn.Alignment = widget.ButtonAlignLeading
		buttons = append(buttons, btn)
	}

	list := container.NewVBox()
	for _, btn := range buttons {
		list.Add(btn)
	}
	return container.NewVScroll(list)
}

func findButton(buttons []*widget.Button, name string) *widget.Button {
	for _, btn := range buttons {
		if btn.Text == name {
			return btn
		}
	}
	return nil
}

// createServiceTop 只创建服务页面上方部分（表单+操作按钮）
func (a *App) createServiceTop(meta config.ServiceMeta) fyne.CanvasObject {
	c, _ := a.registry.Get(meta.Type)

	title := widget.NewLabel("🔌 " + meta.Name + " 连接测试")
	title.TextStyle = fyne.TextStyle{Bold: true}

	fw := newFormWidgets(meta, a.window, c, a.resultLabel, a.resultScroll)
	formSection := fw.buildForm()
	actionSection := fw.buildActions(meta, c)

	return container.NewVScroll(container.NewVBox(
		title,
		widget.NewSeparator(),
		formSection,
		widget.NewSeparator(),
		actionSection,
	))
}
