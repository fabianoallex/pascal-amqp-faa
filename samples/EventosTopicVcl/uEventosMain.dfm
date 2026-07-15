object frmEventos: TfrmEventos
  Left = 0
  Top = 0
  Caption = 'Eventos (exchange topic)'
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
    Height = 120
    Caption = ' Publicar evento '
    TabOrder = 1
    object lblExchange: TLabel
      Left = 16
      Top = 28
      Width = 91
      Height = 15
      Caption = 'Exchange (topic):'
    end
    object lblRoutingKey: TLabel
      Left = 16
      Top = 57
      Width = 66
      Height = 15
      Caption = 'Routing key:'
    end
    object lblMensagem: TLabel
      Left = 16
      Top = 86
      Width = 62
      Height = 15
      Caption = 'Mensagem:'
    end
    object edtExchange: TEdit
      Left = 120
      Top = 24
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'notas-eventos'
    end
    object edtRoutingKey: TEdit
      Left = 120
      Top = 53
      Width = 160
      Height = 23
      TabOrder = 1
      Text = 'nota.aprovada'
    end
    object edtMensagem: TEdit
      Left = 120
      Top = 82
      Width = 160
      Height = 23
      TabOrder = 2
    end
    object btnPublicar: TButton
      Left = 292
      Top = 80
      Width = 95
      Height = 25
      Caption = 'Publicar'
      Enabled = False
      TabOrder = 3
      OnClick = btnPublicarClick
    end
  end
  object gbAssinar: TGroupBox
    Left = 416
    Top = 186
    Width = 276
    Height = 120
    Caption = ' Assinantes '
    TabOrder = 2
    object lblBindingKey: TLabel
      Left = 12
      Top = 28
      Width = 65
      Height = 15
      Caption = 'Binding key:'
    end
    object edtBindingKey: TEdit
      Left = 90
      Top = 24
      Width = 170
      Height = 23
      TabOrder = 0
      Text = 'nota.#'
    end
    object btnAssinar: TButton
      Left = 12
      Top = 56
      Width = 100
      Height = 25
      Caption = 'Assinar'
      Enabled = False
      TabOrder = 1
      OnClick = btnAssinarClick
    end
    object btnRemover: TButton
      Left = 120
      Top = 56
      Width = 140
      Height = 25
      Caption = 'Remover selecionado'
      Enabled = False
      TabOrder = 2
      OnClick = btnRemoverClick
    end
  end
  object lvAssinantes: TListView
    Left = 8
    Top = 314
    Width = 684
    Height = 90
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'N'#186
        Width = 40
      end
      item
        Caption = 'Binding key'
        Width = 150
      end
      item
        Caption = 'Fila'
        Width = 250
      end
      item
        Caption = 'Recebidas'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object lvEventos: TListView
    Left = 8
    Top = 410
    Width = 684
    Height = 120
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'Hora'
        Width = 90
      end
      item
        Caption = 'Assinante'
        Width = 150
      end
      item
        Caption = 'Routing key'
        Width = 140
      end
      item
        Caption = 'Mensagem'
        Width = 220
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 4
    ViewStyle = vsReport
  end
  object btnLimparLista: TButton
    Left = 480
    Top = 536
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar eventos'
    TabOrder = 5
    OnClick = btnLimparListaClick
  end
  object btnLimparLog: TButton
    Left = 590
    Top = 536
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar log'
    TabOrder = 6
    OnClick = btnLimparLogClick
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
