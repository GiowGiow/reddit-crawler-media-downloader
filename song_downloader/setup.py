from setuptools import find_packages, setup

setup(
    name="suno_downloader",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.2.0",
        "tqdm>=4.60.0",
    ],
    entry_points={
        "console_scripts": [
            "suno-downloader=suno_downloader.main:main",
        ],
    },
    author="Suno Downloader Contributors",
    description="A tool for downloading songs from Suno AI",
    keywords="suno, music, downloader",
    python_requires=">=3.6",
)
