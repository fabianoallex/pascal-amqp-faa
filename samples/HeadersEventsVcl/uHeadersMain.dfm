object frmHeaders: TfrmHeaders
  Left = 0
  Top = 0
  Caption = 'Events (headers exchange)'
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
  object gbPublish: TGroupBox
    Left = 8
    Top = 186
    Width = 400
    Height = 120
    Caption = ' Publish event '
    TabOrder = 1
    object lblExchange: TLabel
      Left = 16
      Top = 28
      Width = 55
      Height = 15
      Caption = 'Exchange:'
    end
    object lblType: TLabel
      Left = 16
      Top = 57
      Width = 26
      Height = 15
      Caption = 'Type:'
    end
    object lblRegion: TLabel
      Left = 205
      Top = 57
      Width = 44
      Height = 15
      Caption = 'Region:'
    end
    object lblMessage: TLabel
      Left = 16
      Top = 86
      Width = 62
      Height = 15
      Caption = 'Message:'
    end
    object edtExchange: TEdit
      Left = 90
      Top = 24
      Width = 190
      Height = 23
      TabOrder = 0
      Text = 'invoice-headers'
    end
    object edtType: TEdit
      Left = 90
      Top = 53
      Width = 90
      Height = 23
      TabOrder = 1
      Text = 'nfe'
    end
    object edtRegion: TEdit
      Left = 255
      Top = 53
      Width = 90
      Height = 23
      TabOrder = 2
      Text = 'south'
    end
    object edtMessage: TEdit
      Left = 90
      Top = 82
      Width = 190
      Height = 23
      TabOrder = 3
    end
    object btnPublish: TButton
      Left = 292
      Top = 80
      Width = 95
      Height = 25
      Caption = 'Publish'
      Enabled = False
      TabOrder = 4
      OnClick = btnPublishClick
    end
  end
  object gbSubscribe: TGroupBox
    Left = 416
    Top = 186
    Width = 276
    Height = 120
    Caption = ' Subscribers '
    TabOrder = 2
    object lblMatch: TLabel
      Left = 12
      Top = 28
      Width = 46
      Height = 15
      Caption = 'x-match:'
    end
    object lblCritType: TLabel
      Left = 150
      Top = 28
      Width = 26
      Height = 15
      Caption = 'Type:'
    end
    object lblCritRegion: TLabel
      Left = 12
      Top = 60
      Width = 44
      Height = 15
      Caption = 'Region:'
    end
    object cmbMatch: TComboBox
      Left = 66
      Top = 24
      Width = 70
      Height = 23
      Style = csDropDownList
      ItemIndex = 0
      TabOrder = 0
      Text = 'all'
      Items.Strings = (
        'all'
        'any')
    end
    object edtCritType: TEdit
      Left = 182
      Top = 24
      Width = 80
      Height = 23
      TabOrder = 1
      Text = 'nfe'
    end
    object edtCritRegion: TEdit
      Left = 66
      Top = 56
      Width = 90
      Height = 23
      TabOrder = 2
      Text = 'south'
    end
    object btnSubscribe: TButton
      Left = 12
      Top = 86
      Width = 100
      Height = 25
      Caption = 'Subscribe'
      Enabled = False
      TabOrder = 3
      OnClick = btnSubscribeClick
    end
    object btnRemove: TButton
      Left = 120
      Top = 86
      Width = 140
      Height = 25
      Caption = 'Remove selected'
      Enabled = False
      TabOrder = 4
      OnClick = btnRemoveClick
    end
  end
  object lvSubscribers: TListView
    Left = 8
    Top = 314
    Width = 684
    Height = 90
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'No.'
        Width = 36
      end
      item
        Caption = 'x-match'
        Width = 56
      end
      item
        Caption = 'Criteria'
        Width = 210
      end
      item
        Caption = 'Queue'
        Width = 300
      end
      item
        Caption = 'Received'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object lvEvents: TListView
    Left = 8
    Top = 410
    Width = 684
    Height = 120
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'Time'
        Width = 90
      end
      item
        Caption = 'Subscriber'
        Width = 170
      end
      item
        Caption = 'Type'
        Width = 90
      end
      item
        Caption = 'Region'
        Width = 90
      end
      item
        Caption = 'Message'
        Width = 220
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 4
    ViewStyle = vsReport
  end
  object btnClearList: TButton
    Left = 480
    Top = 536
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear events'
    TabOrder = 5
    OnClick = btnClearListClick
  end
  object btnClearLog: TButton
    Left = 590
    Top = 536
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Clear log'
    TabOrder = 6
    OnClick = btnClearLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 564
    Width = 684
    Height = 68
    Anchors = [akLeft, akTop, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 7
  end
end
