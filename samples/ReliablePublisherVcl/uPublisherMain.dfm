object frmPublisher: TfrmPublisher
  Left = 0
  Top = 0
  Caption = 'Reliable Publisher (VCL)'
  ClientHeight = 640
  ClientWidth = 700
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnClose = FormClose
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  DesignSize = (
    700
    640)
  TextHeight = 15
  object gbConnection: TGroupBox
    Left = 8
    Top = 8
    Width = 684
    Height = 170
    Caption = ' Connection '
    TabOrder = 0
    object lblHost: TLabel
      Left = 16
      Top = 28
      Width = 28
      Height = 15
      Caption = 'Host:'
    end
    object lblPort: TLabel
      Left = 270
      Top = 28
      Width = 31
      Height = 15
      Caption = 'Port:'
    end
    object lblVHost: TLabel
      Left = 16
      Top = 57
      Width = 35
      Height = 15
      Caption = 'VHost:'
    end
    object lblUser: TLabel
      Left = 270
      Top = 57
      Width = 43
      Height = 15
      Caption = 'User:'
    end
    object lblPassword: TLabel
      Left = 16
      Top = 86
      Width = 35
      Height = 15
      Caption = 'Password:'
    end
    object lblStatus: TLabel
      Left = 140
      Top = 145
      Width = 79
      Height = 15
      Caption = 'Disconnected'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clRed
      Font.Height = -12
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object edtHost: TEdit
      Left = 90
      Top = 24
      Width = 150
      Height = 23
      TabOrder = 0
      Text = 'localhost'
    end
    object edtPort: TEdit
      Left = 320
      Top = 24
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '5672'
    end
    object edtVHost: TEdit
      Left = 90
      Top = 53
      Width = 150
      Height = 23
      TabOrder = 2
      Text = '/'
    end
    object edtUser: TEdit
      Left = 330
      Top = 53
      Width = 130
      Height = 23
      TabOrder = 3
      Text = 'guest'
    end
    object edtPassword: TEdit
      Left = 90
      Top = 82
      Width = 150
      Height = 23
      PasswordChar = '*'
      TabOrder = 4
      Text = 'guest'
    end
    object chkUseTls: TCheckBox
      Left = 270
      Top = 85
      Width = 190
      Height = 17
      Caption = 'Use TLS (amqps)'
      TabOrder = 5
      OnClick = chkUseTlsClick
    end
    object chkTlsVerifyPeer: TCheckBox
      Left = 270
      Top = 108
      Width = 190
      Height = 17
      Caption = 'Verify broker certificate'
      Checked = True
      State = cbChecked
      TabOrder = 6
    end
    object chkRepublish: TCheckBox
      Left = 16
      Top = 112
      Width = 250
      Height = 17
      Caption = 'Resend unconfirmed on reconnect'
      Checked = True
      State = cbChecked
      TabOrder = 7
    end
    object btnConnect: TButton
      Left = 16
      Top = 140
      Width = 110
      Height = 25
      Caption = 'Connect'
      TabOrder = 8
      OnClick = btnConnectClick
    end
  end
  object gbPublishing: TGroupBox
    Left = 8
    Top = 186
    Width = 684
    Height = 92
    Caption = ' Publishing (confirm mode) '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 28
      Width = 21
      Height = 15
      Caption = 'Queue:'
    end
    object lblCount: TLabel
      Left = 285
      Top = 28
      Width = 24
      Height = 15
      Caption = 'Count:'
    end
    object lblInterval: TLabel
      Left = 380
      Top = 28
      Width = 77
      Height = 15
      Caption = 'Interval (ms):'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 24
      Width = 180
      Height = 23
      TabOrder = 0
      Text = 'authorizer-replies'
    end
    object edtCount: TEdit
      Left = 320
      Top = 24
      Width = 45
      Height = 23
      TabOrder = 1
      Text = '30'
    end
    object edtInterval: TEdit
      Left = 462
      Top = 24
      Width = 50
      Height = 23
      TabOrder = 2
      Text = '300'
    end
    object btnPublish: TButton
      Left = 16
      Top = 56
      Width = 120
      Height = 25
      Caption = 'Publish batch'
      Enabled = False
      TabOrder = 3
      OnClick = btnPublishClick
    end
    object btnNoRoute: TButton
      Left = 150
      Top = 56
      Width = 190
      Height = 25
      Caption = 'Publish unroutable (mandatory)'
      Enabled = False
      TabOrder = 4
      OnClick = btnNoRouteClick
    end
  end
  object lvMessages: TListView
    Left = 8
    Top = 286
    Width = 684
    Height = 190
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'No.'
        Width = 46
      end
      item
        Caption = 'Key'
        Width = 190
      end
      item
        Caption = 'Status'
        Width = 190
      end
      item
        Caption = 'Sent'
        Width = 80
      end
      item
        Caption = 'Resolved'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 2
    ViewStyle = vsReport
  end
  object lblTotals: TLabel
    Left = 8
    Top = 484
    Width = 63
    Height = 15
    Caption = 'Published: 0'
  end
  object btnClearList: TButton
    Left = 480
    Top = 480
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear list'
    TabOrder = 3
    OnClick = btnClearListClick
  end
  object btnClearLog: TButton
    Left = 590
    Top = 480
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear log'
    TabOrder = 4
    OnClick = btnClearLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 508
    Width = 684
    Height = 124
    Anchors = [akLeft, akTop, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 5
  end
end
