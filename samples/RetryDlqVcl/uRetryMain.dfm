object frmRetry: TfrmRetry
  Left = 0
  Top = 0
  Caption = 'Retry + DLQ (dead-letter)'
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
  object gbTopology: TGroupBox
    Left = 8
    Top = 186
    Width = 335
    Height = 120
    Caption = ' Topology (recreated on connect) '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 28
      Width = 51
      Height = 15
      Caption = 'Base queue:'
    end
    object lblBackoff: TLabel
      Left = 16
      Top = 57
      Width = 68
      Height = 15
      Caption = 'Backoff (ms):'
    end
    object lblMax: TLabel
      Left = 170
      Top = 57
      Width = 80
      Height = 15
      Caption = 'Max attempts:'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 24
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'invoice-processing'
    end
    object edtBackoff: TEdit
      Left = 96
      Top = 53
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '4000'
    end
    object edtMax: TEdit
      Left = 256
      Top = 53
      Width = 40
      Height = 23
      TabOrder = 2
      Text = '3'
    end
  end
  object gbProcess: TGroupBox
    Left = 351
    Top = 186
    Width = 341
    Height = 120
    Caption = ' Simulated processing '
    TabOrder = 2
    object lblFailFirst: TLabel
      Left = 12
      Top = 28
      Width = 88
      Height = 15
      Caption = 'Fail the first:'
    end
    object lblCount: TLabel
      Left = 190
      Top = 28
      Width = 24
      Height = 15
      Caption = 'Count:'
    end
    object lblCounters: TLabel
      Left = 12
      Top = 92
      Width = 150
      Height = 15
      Caption = 'Published: 0'
    end
    object edtFailFirst: TEdit
      Left = 110
      Top = 24
      Width = 40
      Height = 23
      TabOrder = 0
      Text = '2'
    end
    object edtCount: TEdit
      Left = 222
      Top = 24
      Width = 40
      Height = 23
      TabOrder = 1
      Text = '3'
    end
    object btnPublish: TButton
      Left = 12
      Top = 56
      Width = 120
      Height = 25
      Caption = 'Publish invoices'
      Enabled = False
      TabOrder = 2
      OnClick = btnPublishClick
    end
    object btnConsume: TButton
      Left = 140
      Top = 56
      Width = 130
      Height = 25
      Caption = 'Start consuming'
      Enabled = False
      TabOrder = 3
      OnClick = btnConsumeClick
    end
  end
  object lvInvoices: TListView
    Left = 8
    Top = 314
    Width = 684
    Height = 168
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'Key'
        Width = 170
      end
      item
        Caption = 'Attempt'
        Width = 70
      end
      item
        Caption = 'Status'
        Width = 270
      end
      item
        Caption = 'Updated'
        Width = 90
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object btnClearList: TButton
    Left = 480
    Top = 488
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear list'
    TabOrder = 4
    OnClick = btnClearListClick
  end
  object btnClearLog: TButton
    Left = 590
    Top = 488
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear log'
    TabOrder = 5
    OnClick = btnClearLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 516
    Width = 684
    Height = 116
    Anchors = [akLeft, akTop, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 6
  end
end
