object frmBackOffice: TfrmBackOffice
  Left = 0
  Top = 0
  Caption = 'BackOffice (VCL)'
  ClientHeight = 740
  ClientWidth = 560
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
    560
    740)
  TextHeight = 15
  object lblTotals: TLabel
    Left = 8
    Top = 641
    Width = 138
    Height = 15
    Anchors = [akLeft, akBottom]
    Caption = 'Received: 0   |   Ready: 0'
  end
  object gbConnection: TGroupBox
    Left = 8
    Top = 8
    Width = 540
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
  object gbConsume: TGroupBox
    Left = 8
    Top = 186
    Width = 540
    Height = 90
    Caption = ' Consuming '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 26
      Width = 21
      Height = 15
      Caption = 'Queue:'
    end
    object lblPrefetch: TLabel
      Left = 310
      Top = 26
      Width = 47
      Height = 15
      Caption = 'Prefetch:'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 22
      Width = 200
      Height = 23
      TabOrder = 0
      Text = 'authorizer-replies'
    end
    object edtPrefetch: TEdit
      Left = 390
      Top = 22
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '10'
    end
    object chkManual: TCheckBox
      Left = 16
      Top = 56
      Width = 300
      Height = 17
      Caption = 'Manual confirmation (accept/reject each invoice)'
      TabOrder = 2
    end
    object chkDedicated: TCheckBox
      Left = 330
      Top = 56
      Width = 200
      Height = 17
      Caption = 'Dedicated thread (guaranteed order)'
      TabOrder = 3
    end
  end
  object gbApproval: TGroupBox
    Left = 8
    Top = 284
    Width = 540
    Height = 60
    Caption = ' Manual approval '
    TabOrder = 2
    object btnAccept: TButton
      Left = 16
      Top = 22
      Width = 110
      Height = 25
      Caption = 'Accept (ACK)'
      Enabled = False
      TabOrder = 0
      OnClick = btnAcceptClick
    end
    object btnReject: TButton
      Left = 134
      Top = 22
      Width = 110
      Height = 25
      Caption = 'Reject (NACK)'
      Enabled = False
      TabOrder = 1
      OnClick = btnRejectClick
    end
    object chkRequeue: TCheckBox
      Left = 254
      Top = 27
      Width = 200
      Height = 17
      Caption = 'Requeue'
      TabOrder = 2
    end
  end
  object lvInvoices: TListView
    Left = 8
    Top = 352
    Width = 538
    Height = 280
    Anchors = [akLeft, akTop, akRight, akBottom]
    Columns = <
      item
        Caption = 'Key'
        Width = 190
      end
      item
        Caption = 'Status'
        Width = 90
      end
      item
        Caption = 'Worker'
        Width = 70
      end
      item
        Caption = 'Received'
        Width = 80
      end
      item
        Caption = 'Ready'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object btnClearLog: TButton
    Left = 450
    Top = 636
    Width = 96
    Height = 23
    Anchors = [akRight, akBottom]
    Caption = 'Clear log'
    TabOrder = 4
    OnClick = btnClearLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 665
    Width = 538
    Height = 67
    Anchors = [akLeft, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 5
  end
  object Button1: TButton
    Left = 348
    Top = 636
    Width = 96
    Height = 23
    Anchors = [akRight, akBottom]
    Caption = 'Clear List'
    TabOrder = 6
    OnClick = Button1Click
  end
end
