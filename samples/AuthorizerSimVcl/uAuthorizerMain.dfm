object frmAuthorizer: TfrmAuthorizer
  Left = 0
  Top = 0
  Caption = 'AuthorizerSim (VCL)'
  ClientHeight = 450
  ClientWidth = 500
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnClose = FormClose
  OnCreate = FormCreate
  DesignSize = (
    500
    450)
  TextHeight = 15
  object gbConnection: TGroupBox
    Left = 8
    Top = 8
    Width = 484
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
    object btnConnect: TButton
      Left = 16
      Top = 140
      Width = 110
      Height = 25
      Caption = 'Connect'
      TabOrder = 7
      OnClick = btnConnectClick
    end
  end
  object gbPublishing: TGroupBox
    Left = 8
    Top = 186
    Width = 484
    Height = 90
    Caption = ' Publishing '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 28
      Width = 21
      Height = 15
      Caption = 'Queue:'
    end
    object lblCount: TLabel
      Left = 310
      Top = 28
      Width = 65
      Height = 15
      Caption = 'Count:'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 24
      Width = 200
      Height = 23
      TabOrder = 0
      Text = 'authorizer-replies'
    end
    object edtCount: TEdit
      Left = 390
      Top = 24
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '6'
    end
    object btnPublish: TButton
      Left = 16
      Top = 56
      Width = 130
      Height = 25
      Caption = 'Publish invoices'
      Enabled = False
      TabOrder = 2
      OnClick = btnPublishClick
    end
  end
  object btnClearLog: TButton
    Left = 390
    Top = 280
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear log'
    TabOrder = 2
    OnClick = btnClearLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 308
    Width = 482
    Height = 134
    Anchors = [akLeft, akTop, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 3
  end
end
