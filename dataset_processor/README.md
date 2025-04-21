# Processo de Classificação de Gêneros Musicais
- Os pesos do modelo de embedding ajudam a extrair características de áudio significativas.
- Os pesos do modelo de classificação determinam como mapear essas características para previsões de gênero.

O processo em duas etapas:
Primeira etapa: converter o áudio em embeddings

    Carregar o áudio usando o MonoLoader (taxa de amostragem de 16 kHz)

    Processá‑lo com o TensorflowPredictEffnetDiscogs para obter um vetor de embedding de 1280 dimensões

Segunda etapa: classificar esses embeddings em gêneros

    Alimentar os embeddings no TensorflowPredict2D

    Obter previsões para todas as 87 classes de gênero

    Cada previsão é uma pontuação de confiança entre 0 e 1

# Para rodar
- Replicar o ambiente do conda com o arquivo environment.yml
```bash
conda env create -f environment.yml
```
- Ativar o ambiente
```bash
conda activate <nome_do_ambiente>
```

Rodar os script para o mvsep ou para o dataset do subreddit