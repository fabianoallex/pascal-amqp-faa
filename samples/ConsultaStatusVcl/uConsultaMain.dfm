object frmConsulta: TfrmConsulta
  Left = 0
  Top = 0
  Caption = 'Consulta de Status (RPC)'
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
  object gbServidor: TGroupBox
    Left = 8
    Top = 186
    Width = 335
    Height = 120
    Caption = ' Servidor (responde consultas) '
    TabOrder = 1
    object lblFilaPedidos: TLabel
      Left = 16
      Top = 28
      Width = 84
      Height = 15
      Caption = 'Fila de pedidos:'
    end
    object lblDelay: TLabel
      Left = 16
      Top = 57
      Width = 93
      Height = 15
      Caption = 'Demora m'#225'x (ms):'
    end
    object lblAtendidas: TLabel
      Left = 170
      Top = 89
      Width = 65
      Height = 15
      Caption = 'Atendidas: 0'
    end
    object edtFilaPedidos: TEdit
      Left = 116
      Top = 24
      Width = 200
      Height = 23
      TabOrder = 0
      Text = 'consulta-status'
    end
    object edtDelay: TEdit
      Left = 116
      Top = 53
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '800'
    end
    object btnServidor: TButton
      Left = 16
      Top = 84
      Width = 130
      Height = 25
      Caption = 'Iniciar servidor'
      Enabled = False
      TabOrder = 2
      OnClick = btnServidorClick
    end
  end
  object gbCliente: TGroupBox
    Left = 351
    Top = 186
    Width = 341
    Height = 120
    Caption = ' Cliente (consulta status) '
    TabOrder = 2
    object lblChave: TLabel
      Left = 12
      Top = 28
      Width = 37
      Height = 15
      Caption = 'Chave:'
    end
    object lblQtdConsultas: TLabel
      Left = 232
      Top = 28
      Width = 24
      Height = 15
      Caption = 'Qtd:'
    end
    object lblTimeout: TLabel
      Left = 12
      Top = 57
      Width = 71
      Height = 15
      Caption = 'Timeout (ms):'
    end
    object edtChave: TEdit
      Left = 60
      Top = 24
      Width = 160
      Height = 23
      TabOrder = 0
      Text = 'NFE-000123'
    end
    object edtQtdConsultas: TEdit
      Left = 264
      Top = 24
      Width = 40
      Height = 23
      TabOrder = 1
      Text = '1'
    end
    object edtTimeout: TEdit
      Left = 92
      Top = 53
      Width = 60
      Height = 23
      TabOrder = 2
      Text = '3000'
    end
    object btnConsultar: TButton
      Left = 12
      Top = 84
      Width = 130
      Height = 25
      Caption = 'Consultar status'
      Enabled = False
      TabOrder = 3
      OnClick = btnConsultarClick
    end
  end
  object lvConsultas: TListView
    Left = 8
    Top = 314
    Width = 684
    Height = 168
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'Chave'
        Width = 150
      end
      item
        Caption = 'Correla'#231#227'o'
        Width = 110
      end
      item
        Caption = 'Status'
        Width = 200
      end
      item
        Caption = 'Enviada'
        Width = 100
      end
      item
        Caption = 'Tempo'
        Width = 90
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 3
    ViewStyle = vsReport
  end
  object btnLimparLista: TButton
    Left = 480
    Top = 488
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar lista'
    TabOrder = 4
    OnClick = btnLimparListaClick
  end
  object btnLimparLog: TButton
    Left = 590
    Top = 488
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar log'
    TabOrder = 5
    OnClick = btnLimparLogClick
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
  object tmrTimeout: TTimer
    Interval = 200
    OnTimer = tmrTimeoutTimer
    Left = 640
    Top = 240
  end
end
