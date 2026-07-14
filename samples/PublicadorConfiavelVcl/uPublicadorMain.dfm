object frmPublicador: TfrmPublicador
  Left = 0
  Top = 0
  Caption = 'Publicador Confi'#225'vel (VCL)'
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
    object chkRepublicar: TCheckBox
      Left = 16
      Top = 112
      Width = 250
      Height = 17
      Caption = 'Reenviar n'#227'o confirmadas na reconex'#227'o'
      Checked = True
      State = cbChecked
      TabOrder = 7
    end
    object btnConectar: TButton
      Left = 16
      Top = 140
      Width = 110
      Height = 25
      Caption = 'Conectar'
      TabOrder = 8
      OnClick = btnConectarClick
    end
  end
  object gbPublicacao: TGroupBox
    Left = 8
    Top = 186
    Width = 684
    Height = 92
    Caption = ' Publica'#231#227'o (confirm mode) '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 28
      Width = 21
      Height = 15
      Caption = 'Fila:'
    end
    object lblQtd: TLabel
      Left = 285
      Top = 28
      Width = 24
      Height = 15
      Caption = 'Qtd:'
    end
    object lblIntervalo: TLabel
      Left = 380
      Top = 28
      Width = 77
      Height = 15
      Caption = 'Intervalo (ms):'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 24
      Width = 180
      Height = 23
      TabOrder = 0
      Text = 'sefaz-respostas'
    end
    object edtQtd: TEdit
      Left = 320
      Top = 24
      Width = 45
      Height = 23
      TabOrder = 1
      Text = '30'
    end
    object edtIntervalo: TEdit
      Left = 462
      Top = 24
      Width = 50
      Height = 23
      TabOrder = 2
      Text = '300'
    end
    object btnPublicar: TButton
      Left = 16
      Top = 56
      Width = 120
      Height = 25
      Caption = 'Publicar lote'
      Enabled = False
      TabOrder = 3
      OnClick = btnPublicarClick
    end
    object btnSemRota: TButton
      Left = 150
      Top = 56
      Width = 190
      Height = 25
      Caption = 'Publicar sem rota (mandatory)'
      Enabled = False
      TabOrder = 4
      OnClick = btnSemRotaClick
    end
  end
  object lvMensagens: TListView
    Left = 8
    Top = 286
    Width = 684
    Height = 190
    Anchors = [akLeft, akTop, akRight]
    Columns = <
      item
        Caption = 'N'#186
        Width = 46
      end
      item
        Caption = 'Chave'
        Width = 190
      end
      item
        Caption = 'Status'
        Width = 190
      end
      item
        Caption = 'Enviada'
        Width = 80
      end
      item
        Caption = 'Resolvida'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 2
    ViewStyle = vsReport
  end
  object lblContagem: TLabel
    Left = 8
    Top = 484
    Width = 63
    Height = 15
    Caption = 'Publicadas: 0'
  end
  object btnLimparLista: TButton
    Left = 480
    Top = 480
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar lista'
    TabOrder = 3
    OnClick = btnLimparListaClick
  end
  object btnLimparLog: TButton
    Left = 590
    Top = 480
    Width = 100
    Height = 23
    Anchors = [akTop, akRight]
    Caption = 'Limpar log'
    TabOrder = 4
    OnClick = btnLimparLogClick
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
