object frmRetaguarda: TfrmRetaguarda
  Left = 0
  Top = 0
  Caption = 'Retaguarda (VCL)'
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
  object lblContagem: TLabel
    Left = 8
    Top = 641
    Width = 138
    Height = 15
    Anchors = [akLeft, akBottom]
    Caption = 'Recebidas: 0   |   Prontas: 0'
  end
  object gbConexao: TGroupBox
    Left = 8
    Top = 8
    Width = 540
    Height = 170
    Caption = ' Conex'#227'o '
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
      Caption = 'Porta:'
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
      Caption = 'Usu'#225'rio:'
    end
    object lblPassword: TLabel
      Left = 16
      Top = 86
      Width = 35
      Height = 15
      Caption = 'Senha:'
    end
    object lblStatus: TLabel
      Left = 140
      Top = 145
      Width = 79
      Height = 15
      Caption = 'Desconectado'
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
      Caption = 'Usar TLS (amqps)'
      TabOrder = 5
      OnClick = chkUseTlsClick
    end
    object chkTlsVerifyPeer: TCheckBox
      Left = 270
      Top = 108
      Width = 190
      Height = 17
      Caption = 'Validar certificado do broker'
      TabOrder = 6
    end
    object btnConectar: TButton
      Left = 16
      Top = 140
      Width = 110
      Height = 25
      Caption = 'Conectar'
      TabOrder = 7
      OnClick = btnConectarClick
    end
  end
  object gbConsumo: TGroupBox
    Left = 8
    Top = 186
    Width = 540
    Height = 90
    Caption = ' Consumo '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 26
      Width = 21
      Height = 15
      Caption = 'Fila:'
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
      Text = 'sefaz-respostas'
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
      Caption = 'Confirma'#231#227'o manual (aceitar/rejeitar cada nota)'
      TabOrder = 2
    end
    object chkDedicado: TCheckBox
      Left = 330
      Top = 56
      Width = 200
      Height = 17
      Caption = 'Thread dedicada (ordem garantida)'
      TabOrder = 3
    end
  end
  object gbAprovacao: TGroupBox
    Left = 8
    Top = 284
    Width = 540
    Height = 60
    Caption = ' Aprova'#231#227'o manual '
    TabOrder = 2
    object btnAceitar: TButton
      Left = 16
      Top = 22
      Width = 110
      Height = 25
      Caption = 'Aceitar (ACK)'
      Enabled = False
      TabOrder = 0
      OnClick = btnAceitarClick
    end
    object btnRejeitar: TButton
      Left = 134
      Top = 22
      Width = 110
      Height = 25
      Caption = 'Rejeitar (NACK)'
      Enabled = False
      TabOrder = 1
      OnClick = btnRejeitarClick
    end
    object chkRequeue: TCheckBox
      Left = 254
      Top = 27
      Width = 200
      Height = 17
      Caption = 'Reencaminhar (requeue)'
      TabOrder = 2
    end
  end
  object lvNotas: TListView
    Left = 8
    Top = 352
    Width = 538
    Height = 280
    Anchors = [akLeft, akTop, akRight, akBottom]
    Columns = <
      item
        Caption = 'Chave'
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
        Caption = 'Recebida'
        Width = 80
      end
      item
        Caption = 'Pronta'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object btnLimparLog: TButton
    Left = 450
    Top = 636
    Width = 96
    Height = 23
    Anchors = [akRight, akBottom]
    Caption = 'Limpar log'
    TabOrder = 4
    OnClick = btnLimparLogClick
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
    Caption = 'Limpar Lista'
    TabOrder = 6
    OnClick = Button1Click
  end
end
