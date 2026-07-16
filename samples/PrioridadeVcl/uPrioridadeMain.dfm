object frmPrioridade: TfrmPrioridade
  Left = 0
  Top = 0
  Caption = 'Fila com prioridade'
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
  DesignSize = (
    700
    640)
  TextHeight = 15
  object gbConexao: TGroupBox
    Left = 8
    Top = 8
    Width = 684
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
      Checked = True
      State = cbChecked
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
  object gbPublicar: TGroupBox
    Left = 8
    Top = 186
    Width = 400
    Height = 150
    Caption = ' Publicar '
    TabOrder = 1
    object lblFila: TLabel
      Left = 16
      Top = 28
      Width = 22
      Height = 15
      Caption = 'Fila:'
    end
    object lblMaxPrio: TLabel
      Left = 16
      Top = 57
      Width = 82
      Height = 15
      Caption = 'M'#225'x prioridade:'
    end
    object lblPrio: TLabel
      Left = 180
      Top = 57
      Width = 57
      Height = 15
      Caption = 'Prioridade:'
    end
    object lblMensagem: TLabel
      Left = 16
      Top = 86
      Width = 62
      Height = 15
      Caption = 'Mensagem:'
    end
    object edtFila: TEdit
      Left = 110
      Top = 24
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'fila-prioridade'
    end
    object edtMaxPrio: TEdit
      Left = 110
      Top = 53
      Width = 40
      Height = 23
      TabOrder = 1
      Text = '9'
    end
    object edtPrio: TEdit
      Left = 250
      Top = 53
      Width = 40
      Height = 23
      TabOrder = 2
      Text = '5'
    end
    object edtMensagem: TEdit
      Left = 110
      Top = 82
      Width = 160
      Height = 23
      TabOrder = 3
    end
    object btnPublicar: TButton
      Left = 16
      Top = 114
      Width = 120
      Height = 25
      Caption = 'Publicar 1'
      Enabled = False
      TabOrder = 4
      OnClick = btnPublicarClick
    end
    object btnBurst: TButton
      Left = 145
      Top = 114
      Width = 200
      Height = 25
      Caption = 'Publicar lote (prio. mistas)'
      Enabled = False
      TabOrder = 5
      OnClick = btnBurstClick
    end
  end
  object gbConsumidor: TGroupBox
    Left = 416
    Top = 186
    Width = 276
    Height = 150
    Caption = ' Consumidor (lento) '
    TabOrder = 2
    object lblDelay: TLabel
      Left = 12
      Top = 28
      Width = 87
      Height = 15
      Caption = 'Delay/msg (ms):'
    end
    object lblPrefetch: TLabel
      Left = 12
      Top = 57
      Width = 50
      Height = 15
      Caption = 'Prefetch:'
    end
    object lblConsumidas: TLabel
      Left = 12
      Top = 88
      Width = 78
      Height = 15
      Caption = 'Consumidas: 0'
    end
    object edtDelay: TEdit
      Left = 120
      Top = 24
      Width = 50
      Height = 23
      TabOrder = 0
      Text = '600'
    end
    object edtPrefetch: TEdit
      Left = 120
      Top = 53
      Width = 40
      Height = 23
      TabOrder = 1
      Text = '1'
    end
    object btnConsumidor: TButton
      Left = 12
      Top = 114
      Width = 200
      Height = 25
      Caption = 'Iniciar consumidor'
      Enabled = False
      TabOrder = 2
      OnClick = btnConsumidorClick
    end
  end
  object lvMensagens: TListView
    Left = 8
    Top = 346
    Width = 684
    Height = 180
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = '# sa'#237'da'
        Width = 60
      end
      item
        Caption = 'Prioridade'
        Width = 80
      end
      item
        Caption = 'Mensagem'
        Width = 430
      end
      item
        Caption = 'Hora'
        Width = 110
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object btnLimparLista: TButton
    Left = 480
    Top = 532
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar lista'
    TabOrder = 4
    OnClick = btnLimparListaClick
  end
  object btnLimparLog: TButton
    Left = 590
    Top = 532
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar log'
    TabOrder = 5
    OnClick = btnLimparLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 560
    Width = 684
    Height = 72
    Anchors = [akLeft, akTop, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 6
  end
end
